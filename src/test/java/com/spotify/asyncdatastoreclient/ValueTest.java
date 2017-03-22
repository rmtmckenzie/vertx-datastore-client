/*
 * Copyright (c) 2011-2015 Spotify AB
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

package com.spotify.asyncdatastoreclient;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

public class ValueTest {

  @Test
  public void testValueBuilder() throws Exception {
    final Value value1 = Value.from("test").build();
    assertEquals("test", value1.getString());

    final Value value2 = Value.from(12345).build();
    assertEquals(12345, value2.getInteger());

    final Value value3 = Value.from(123456789L).build();
    assertEquals(123456789L, value3.getInteger());

    final Value value4 = Value.from(1.234).build();
    assertEquals(1.234, value4.getDouble(), 0.01);

    final Value value5 = Value.from(1.234f).build();
    assertEquals(1.234, value5.getDouble(), 0.01);

    final Value value6 = Value.from(true).build();
    assertTrue(value6.getBoolean());

    final Date now = new Date();
    final Value value7 = Value.from(now).build();
    assertEquals(now, value7.getDate());

    final ByteString blob = ByteString.copyFrom(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9});
    final Value value8 = Value.from(blob).build();
    assertTrue(blob.equals(value8.getBlob()));

    final Entity entity9 = Entity.builder("employee", 1234567L)
        .property("fullname", "Fred Blinge")
        .build();
    final Value value9 = Value.from(entity9).build();
    assertEquals("Fred Blinge", value9.getEntity().getString("fullname"));

    final Key key10 = Key.builder("employee", 1234567L).build();
    final Value value10 = Value.from(key10).build();
    assertEquals(1234567L, value10.getKey().getId().longValue());

    final Value value11 = Value.builder(ImmutableList.of("fred", "jack")).build();
    assertTrue(Iterables.elementsEqual(value11.getList(String.class), ImmutableList.of("fred", "jack")));

    final Value value12 = Value.builder(ImmutableList.of("fred", "jack")).build();
    assertTrue(Iterables.elementsEqual(value12.getList(), ImmutableList.of(
        Value.from("fred").build(), Value.from("jack").build())));

    final Value value13 = Value.from("indexed").indexed(true).build();
    assertTrue(value13.isIndexed());

    final Value value14 = Value.from("not_indexed").indexed(false).build();
    assertFalse(value14.isIndexed());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidType() {
    Value.builder('a').build();
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidListType() {
    Value.builder(ImmutableList.of('a')).build();
  }

  @Test
  public void testEquals() throws Exception {
    final Value value1 = Value.from("test").build();
    final Value value2 = Value.from("test").build();
    assertTrue(value1.equals(value2));
    assertTrue(value2.equals(value1));

    final Value value3 = Value.from(1.234).build();
    final Value value4 = Value.from(1.234).build();
    assertTrue(value3.equals(value4));
    assertTrue(value4.equals(value3));

    final Value value5 = Value.builder(ImmutableList.of("fred", "jack")).build();
    final Value value6 = Value.builder(ImmutableList.of("fred", "jack")).build();
    assertTrue(value5.equals(value6));
    assertTrue(value6.equals(value5));

    final Value value7 = Value.from("test").indexed(true).build();
    final Value value8 = Value.from("test").indexed(true).build();
    assertTrue(value7.equals(value8));
    assertTrue(value8.equals(value7));

    final Value value9 = Value.from("test").indexed(true).build();
    final Value value10 = Value.from("test").indexed(false).build();
    assertFalse(value9.equals(value10));
    assertFalse(value10.equals(value9));

    final Value value11 = Value.from(1.234).build();
    final Value value12 = Value.builder(ImmutableList.of("fred", "jack")).build();
    assertFalse(value11.equals(value12));
    assertFalse(value12.equals(value11));
  }

  @Test
  public void testHashCode() throws Exception {
    final Value value1 = Value.from("test").build();
    final Value value2 = Value.from("test").build();
    assertEquals(value1.hashCode(), value2.hashCode());

    final Value value3 = Value.from("test").indexed(false).build();
    final Value value4 = Value.from("test").indexed(true).build();
    assertNotEquals(value3.hashCode(), value4.hashCode());
  }

  @Test
  public void testToString() throws Exception {
    final Value value1 = Value.from("test").build();
    assertEquals("test", value1.toString());

    final Value value2 = Value.from(1.234).build();
    assertEquals("1.234", value2.toString());

    final Value value3 = Value.from(false).build();
    assertEquals("false", value3.toString());

    final Entity entity4 = Entity.builder("employee", 1234567L)
        .property("fullname", "Fred Blinge")
        .build();
    final Value value4 = Value.from(entity4).build();
    assertEquals("{fullname:Fred Blinge}", value4.toString());

    final Key key5 = Key.builder("employee", 1234567L).build();
    final Value value5 = Value.from(key5).build();
    assertEquals("{employee:1234567}", value5.toString());

    final Value value6 = Value.builder(ImmutableList.of("fred", "jack")).build();
    assertEquals("[fred, jack]", value6.toString());
  }

  @Test
  public void testTypeSafeBuilder() {
    assertTrue(Value.from(Value.from("hello").build(), Value.from("world").build())
        .build().isList());
    assertTrue(Value
        .from(ImmutableList.of(Value.from("hello").build(), Value.from("world").build()))
        .build().isList());
    assertTrue(Value.from(Key.builder("foo").build()).build().isKey());
    assertTrue(Value.from("foo").build().isString());
    assertTrue(Value.from(12).build().isInteger());
    assertTrue(Value.from(12L).build().isInteger());
    assertTrue(Value.from(new Date(42L)).build().isDate());
    assertTrue(Value.from(12D).build().isDouble());
    assertTrue(Value.from(12F).build().isDouble());
    assertTrue(Value.from(true).build().isBoolean());
    assertTrue(Value.from(ByteString.copyFrom(new byte[0])).build().isBlob());
    assertTrue(Value.from(Entity.builder().build()).build().isEntity());
  }
}
