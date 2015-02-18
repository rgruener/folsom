/*
 * Copyright (c) 2014-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.folsom.client.ascii;

import com.google.common.base.Charsets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class AsciiMemcacheDecoder extends ByteToMessageDecoder {

  private final ByteArrayOutputStream line = new ByteArrayOutputStream();
  private boolean consumed = false;
  private boolean valueMode = false;

  private ValueAsciiResponse valueResponse = new ValueAsciiResponse();

  private byte[] key = null;
  private byte[] value = null;
  private long cas = 0;
  private int valueOffset;

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf buf,
                        final List<Object> out) throws Exception {
    while (true) {
      int readableBytes = buf.readableBytes();
      if (readableBytes == 0) {
        return;
      }

      if (key != null) {
        final int toCopy = Math.min(value.length - valueOffset, readableBytes);
        if (toCopy > 0) {
          buf.readBytes(value, valueOffset, toCopy);
          readableBytes -= toCopy;
          valueOffset += toCopy;
          if (valueOffset < value.length) {
            return;
          }
        }
        final byte[] line = readLine(buf, readableBytes).toByteArray();
        final String errorLine = new String(line, Charsets.US_ASCII);
        if (line.length > 0) {
          throw new IOException(String.format("Unexpected end of data block: %s", errorLine));
        }
        valueResponse.addGetResult(key, value, cas);
        key = null;
        value = null;
        cas = 0;
      } else {
        final byte[] line = readLine(buf, readableBytes).toByteArray();
        final String errorLine = new String(line, Charsets.US_ASCII);

        final int firstEnd = endIndex(line, 0);
        if (firstEnd < 1) {
          throw new IOException("Unexpected line: " + errorLine);
        }

        final char firstChar = (char) line[0];

        if (Character.isDigit(firstChar)) {
          try {
            long numeric = Long.valueOf(new String(line));
            out.add(new NumericAsciiResponse(numeric));
          } catch (NumberFormatException e) {
            throw new IOException("Unexpected line: " + errorLine, e);
          }
        } else if (firstEnd == 3) {
          expect(line, "END");
          out.add(valueResponse);
          valueResponse = new ValueAsciiResponse();
          valueMode = false;
          return;
        } else if (firstEnd == 5) {
          expect(line, "VALUE");
          valueMode = true;
          // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
          final int keyStart = firstEnd + 1;
          final int keyEnd = endIndex(line, keyStart);
          final byte[] key = Arrays.copyOfRange(line, keyStart, keyEnd);
          if (key.length == 0) {
            throw new IOException("Unexpected line: " + errorLine);
          }

          final int flagsStart = keyEnd + 1;
          final int flagsEnd = endIndex(line, flagsStart);
          if (flagsEnd <= flagsStart) {
            throw new IOException("Unexpected line: " + errorLine);
          }

          final int sizeStart = flagsEnd + 1;
          final int sizeEnd = endIndex(line, sizeStart);
          if (sizeEnd <= sizeStart) {
            throw new IOException("Unexpected line: " + errorLine);
          }
          final int size = (int) parseLong(line, sizeStart, sizeEnd);

          final int casStart = sizeEnd + 1;
          final int casEnd = endIndex(line, casStart);
          long cas = 0;
          if (casStart < casEnd) {
            cas = parseLong(line, casStart, casEnd);
          }
          this.key = key;
          this.value = new byte[size];
          this.valueOffset = 0;
          this.cas = cas;
        } else if (valueMode) {
          // when in valueMode, the only valid responses are "END" and "VALUE"
          throw new IOException("Unexpected line: " + errorLine);
        } else if (firstEnd == 6) {
          if (firstChar == 'S') {
            expect(line, "STORED");
            out.add(AsciiResponse.STORED);
            return;
          } else {
            expect(line, "EXISTS");
            out.add(AsciiResponse.EXISTS);
            return;
          }
        } else if (firstEnd == 7) {
          if (firstChar == 'T') {
            expect(line, "TOUCHED");
            out.add(AsciiResponse.TOUCHED);
            return;
          } else {
            expect(line, "DELETED");
            out.add(AsciiResponse.DELETED);
            return;
          }
        } else if (firstEnd == 9) {
          expect(line, "NOT_FOUND");
          out.add(AsciiResponse.NOT_FOUND);
          return;
        } else if (firstEnd == 10) {
          expect(line, "NOT_STORED");
          out.add(AsciiResponse.NOT_STORED);
          return;
        } else {
          throw new IOException("Unexpected line: " + errorLine);
        }
      }
    }
  }

  private void expect(final byte[] line, final String compareTo) throws IOException {
    final int length = compareTo.length();
    for (int i = 0; i < length; i++) {
      if (line[i] != compareTo.charAt(i)) {
        throw new IOException("Unexpected line: " + new String(line));
      }
    }
  }

  private long parseLong(final byte[] line,
                         final int from, final int to) throws IOException {
    long res = 0;
    for (int i = from; i < to; i++) {
      final int digit = line[i] - '0';
      if (digit < 0 || digit > 9) {
        throw new IOException("Unexpected line: " + new String(line));
      }
      res *= 10;
      res += digit;
    }
    return res;
  }

  private int endIndex(final byte[] line, final int from) {
    final int length = line.length;
    for (int i = from; i < length; i++) {
      if (line[i] == ' ') {
        return i;
      }
    }
    return length;
  }

  private ByteArrayOutputStream readLine(final ByteBuf buf,
                                         final int available) throws IOException {
    if (consumed) {
      line.reset();
      consumed = false;
    }
    for (int i = 0; i < available - 1; i++) {
      final char b = (char) buf.readUnsignedByte();
      if (b == '\r') {
        if (buf.readUnsignedByte() == '\n') {
          consumed = true;
          return line;
        }
        throw new IOException("Expected newline, got something else");
      }
      line.write(b);
    }
    return null;
  }

}
