/*
Copyright 2026 Google LLC

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AdapterOptionsTest {

  @Test
  public void testDefaultInstanceType_isCloud() {
    AdapterOptions options = AdapterOptions.newBuilder().build();

    assertThat(options.getInstanceType()).isEqualTo(InstanceType.CLOUD);
  }

  @Test
  public void testSetSpannerEndpointAndInstanceType_newWay() {
    AdapterOptions options =
        AdapterOptions.newBuilder()
            .spannerEndpoint("omni-host:15000")
            .setInstanceType(InstanceType.OMNI)
            .build();

    assertThat(options.getSpannerEndpoint()).isEqualTo("omni-host:15000");
    assertThat(options.getInstanceType()).isEqualTo(InstanceType.OMNI);
  }
}
