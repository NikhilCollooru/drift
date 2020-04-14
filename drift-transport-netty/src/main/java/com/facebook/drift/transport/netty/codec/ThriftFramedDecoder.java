/*
 * Copyright (C) 2018 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.drift.transport.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

class ThriftFramedDecoder
        extends ByteToMessageDecoder
{
    private final FrameInfoDecoder frameInfoDecoder;
    private final int maxFrameSizeInBytes;

    private Optional<FrameInfo> tooLongFrameInfo = Optional.empty();
    private long tooLongFrameSizeInBytes;
    private long bytesToDiscard;
    private CompositeByteBuf compositeByteBuf;
    private int remainingLength;

    public ThriftFramedDecoder(FrameInfoDecoder frameInfoDecoder, int maxFrameSizeInBytes)
    {
        this.frameInfoDecoder = requireNonNull(frameInfoDecoder, "sequenceIdDecoder is null");
        checkArgument(maxFrameSizeInBytes >= 0, "maxFrameSizeInBytes");
        this.maxFrameSizeInBytes = maxFrameSizeInBytes;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause)
    {
        onError();
    }

    @Override
    public void channelInactive(ChannelHandlerContext context)
    {
        onError();
    }

    private void onError()
    {
        if (compositeByteBuf != null && compositeByteBuf.refCnt() > 0) {
            compositeByteBuf.release();
        }
    }

    @Override
    protected void decode(ChannelHandlerContext context, ByteBuf buffer, List<Object> output)
    {
        decode(context.alloc(), buffer).ifPresent(output::add);
    }

    private Optional<ByteBuf> decode(ByteBufAllocator bufAllocator, ByteBuf buffer)
    {
        if (bytesToDiscard > 0) {
            discardTooLongFrame(buffer);
            return Optional.empty();
        }

        int initialReaderIndex = buffer.readerIndex();

        if (remainingLength == 0) {
            if (buffer.readableBytes() < Integer.BYTES) {
                return Optional.empty();
            }
            remainingLength = (int) buffer.readUnsignedInt();
            compositeByteBuf = bufAllocator.compositeDirectBuffer(8192);

            if (remainingLength > maxFrameSizeInBytes) {
                // this invocation doesn't move the readerIndex
                Optional<FrameInfo> frameInfo = frameInfoDecoder.tryDecodeFrameInfo(bufAllocator, buffer);
                compositeByteBuf.release();
                if (frameInfo.isPresent()) {
                    tooLongFrameInfo = frameInfo;
                    tooLongFrameSizeInBytes = remainingLength;
                    bytesToDiscard = remainingLength;
                    remainingLength = 0;
                    discardTooLongFrame(buffer);
                    return Optional.empty();
                }
                // Basic frame info cannot be decoded and the max frame size is already exceeded.
                // Instead of waiting forever, fail without providing the sequence ID.
                if (buffer.readableBytes() >= maxFrameSizeInBytes) {
                    tooLongFrameInfo = Optional.empty();
                    tooLongFrameSizeInBytes = remainingLength;
                    bytesToDiscard = remainingLength;
                    remainingLength = 0;
                    discardTooLongFrame(buffer);
                    return Optional.empty();
                }
                remainingLength = 0;
                buffer.readerIndex(initialReaderIndex);
                return Optional.empty();
            }
        }

        int currentReadSize = min(buffer.readableBytes(), remainingLength);
        expand(compositeByteBuf, currentReadSize);
        remainingLength -= currentReadSize;
        while (currentReadSize > 0) {
            int size = min(currentReadSize, 65536);
            compositeByteBuf.writeBytes(buffer, buffer.readerIndex(), size);
            buffer.readerIndex(buffer.readerIndex() + size);
            currentReadSize -= size;
        }

        if (remainingLength == 0) {
            ByteBuf frame = compositeByteBuf.retainedDuplicate();
            compositeByteBuf.release();
            return Optional.of(frame);
            //return Optional.of(compositeByteBuf);
        }

        return Optional.empty();
    }

    private void expand(CompositeByteBuf buf, int minWritableBytes)
    {
        int writerIndex = buf.writerIndex();
        int maxCapacity = buf.maxCapacity();
        int oldCapacity = buf.capacity();

        int targetCapacity = writerIndex + minWritableBytes;
        if (targetCapacity <= buf.capacity()) {
            return;
        }

        // Normalize the target capacity to the power of 2.
        int fastWritable = buf.maxFastWritableBytes();
        int newCapacity = fastWritable >= minWritableBytes ? writerIndex + fastWritable
                : calculateNewCapacity(targetCapacity, maxCapacity);

        // Adjust to the new capacity.
        if (newCapacity > oldCapacity) {
            int paddingLength = newCapacity - oldCapacity;
            while (paddingLength > 0) {
                int currentSize = min(paddingLength, 65536);
                ByteBuf padding = buf.alloc().directBuffer(currentSize).setIndex(0, currentSize);
                buf.addComponent(false, padding);
                paddingLength -= currentSize;
            }
        }
    }

    private int calculateNewCapacity(int minNewCapacity, int maxCapacity)
    {
        final int threshold = 1048576 * 4; // 4 MiB page

        if (minNewCapacity == threshold) {
            return threshold;
        }

        // If over threshold, do not double but just increase by threshold.
        if (minNewCapacity > threshold) {
            int newCapacity = minNewCapacity / threshold * threshold;
            if (newCapacity > maxCapacity - threshold) {
                newCapacity = maxCapacity;
            }
            else {
                newCapacity += threshold;
            }
            return newCapacity;
        }

        // Not over threshold. Double up to 4 MiB, starting from 64.
        int newCapacity = 64;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        return Math.min(newCapacity, maxCapacity);
    }

//    @Override
//    protected void decode(ChannelHandlerContext context, ByteBuf buffer, List<Object> output)
//    {
//        decode(context.alloc(), buffer).ifPresent(output::add);
//    }
//
//    private Optional<ByteBuf> decode(ByteBufAllocator bufAllocator, ByteBuf buffer)
//    {
//        if (bytesToDiscard > 0) {
//            discardTooLongFrame(buffer);
//            return Optional.empty();
//        }
//
//        int initialReaderIndex = buffer.readerIndex();
//
//        if (buffer.readableBytes() < Integer.BYTES) {
//            return Optional.empty();
//        }
//        long frameSizeInBytes = buffer.readUnsignedInt();
//
//        if (frameSizeInBytes > maxFrameSizeInBytes) {
//            // this invocation doesn't move the readerIndex
//            Optional<FrameInfo> frameInfo = frameInfoDecoder.tryDecodeFrameInfo(bufAllocator, buffer);
//            if (frameInfo.isPresent()) {
//                tooLongFrameInfo = frameInfo;
//                tooLongFrameSizeInBytes = frameSizeInBytes;
//                bytesToDiscard = frameSizeInBytes;
//                discardTooLongFrame(buffer);
//                return Optional.empty();
//            }
//            // Basic frame info cannot be decoded and the max frame size is already exceeded.
//            // Instead of waiting forever, fail without providing the sequence ID.
//            if (buffer.readableBytes() >= maxFrameSizeInBytes) {
//                tooLongFrameInfo = Optional.empty();
//                tooLongFrameSizeInBytes = frameSizeInBytes;
//                bytesToDiscard = frameSizeInBytes;
//                discardTooLongFrame(buffer);
//                return Optional.empty();
//            }
//            buffer.readerIndex(initialReaderIndex);
//            return Optional.empty();
//        }
//
//        if (buffer.readableBytes() >= frameSizeInBytes) {
//            // toIntExact must be safe, as frameSizeInBytes <= maxFrameSize
//            ByteBuf frame = buffer.retainedSlice(buffer.readerIndex(), toIntExact(frameSizeInBytes));
//            buffer.readerIndex(buffer.readerIndex() + toIntExact(frameSizeInBytes));
//            return Optional.of(frame);
//        }
//
//        buffer.readerIndex(initialReaderIndex);
//        return Optional.empty();
//    }

    private void discardTooLongFrame(ByteBuf buffer)
    {
        // readableBytes returns int, toIntExact must be safe
        int bytesToSkip = toIntExact(min(bytesToDiscard, buffer.readableBytes()));
        buffer.skipBytes(bytesToSkip);
        bytesToDiscard -= bytesToSkip;

        if (bytesToDiscard == 0) {
            RuntimeException exception = new FrameTooLargeException(tooLongFrameInfo, tooLongFrameSizeInBytes, maxFrameSizeInBytes);
            tooLongFrameInfo = Optional.empty();
            tooLongFrameSizeInBytes = 0;
            throw exception;
        }
    }
}
