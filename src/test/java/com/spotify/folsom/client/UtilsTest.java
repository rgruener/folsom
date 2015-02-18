/*
 * Copyright (c) 2015 Spotify AB
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
package com.spotify.folsom.client;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;

public class UtilsTest {

  @Test
  public void testValidateKey() throws Exception {
    Utils.validateKey(utf8("hello"));
  }

  @Test
  public void testValidateKeyUTF16() throws Exception {
    Utils.validateKey(utf16("hello"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testValidateKeyTooLongKey() throws Exception {
    Utils.validateKey(utf8(Strings.repeat("hello", 100)));
  }

  @Test
  public void testValidateKeyUTF8() throws Exception {
    Utils.validateKey(utf8("hällo"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testValidateKeyWithSpace() throws Exception {
    Utils.validateKey(utf8("hello world"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testValidateKeyWithSpaceUTF16() throws Exception {
    Utils.validateKey(utf16("hello world"));
  }

  private byte[] utf8(final String key) {
    return key.getBytes(Charsets.UTF_8);
  }

  private byte[] utf16(final String key) {
    return key.getBytes(Charsets.UTF_16);
  }
}
