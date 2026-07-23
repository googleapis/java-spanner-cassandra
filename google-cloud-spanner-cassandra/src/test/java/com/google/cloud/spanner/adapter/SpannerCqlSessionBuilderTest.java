/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.google.cloud.spanner.adapter;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.adapter.SpannerCqlSessionBuilder.InstanceType;
import java.lang.reflect.Field;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerCqlSessionBuilderTest {

  @Test
  public void testSetExperimentalHostEndpoint_deprecatedButWorks() throws Exception {
    SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();
    builder.setExperimentalHostEndpoint("experimental-host:15000");

    Field hostField = SpannerCqlSessionBuilder.class.getDeclaredField("host");
    hostField.setAccessible(true);
    String host = (String) hostField.get(builder);
    assertThat(host).isEqualTo("experimental-host:15000");

    Field instanceTypeField = SpannerCqlSessionBuilder.class.getDeclaredField("instanceType");
    instanceTypeField.setAccessible(true);
    InstanceType instanceType = (InstanceType) instanceTypeField.get(builder);
    assertThat(instanceType).isEqualTo(InstanceType.OMNI);
  }

  @Test
  public void testSetHostAndType_newWay() throws Exception {
    SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();
    builder.setHost("omni-host:15000");
    builder.setInstanceType(InstanceType.OMNI);

    Field hostField = SpannerCqlSessionBuilder.class.getDeclaredField("host");
    hostField.setAccessible(true);
    String host = (String) hostField.get(builder);
    assertThat(host).isEqualTo("omni-host:15000");

    Field instanceTypeField = SpannerCqlSessionBuilder.class.getDeclaredField("instanceType");
    instanceTypeField.setAccessible(true);
    InstanceType instanceType = (InstanceType) instanceTypeField.get(builder);
    assertThat(instanceType).isEqualTo(InstanceType.OMNI);
  }
}
