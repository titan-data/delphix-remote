/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.delphix.server

import io.kotlintest.TestCaseOrder
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.impl.annotations.SpyK
import kotlin.IllegalArgumentException

class DelphixRemoteServerTest : StringSpec() {

    @SpyK
    var client = DelphixRemoteServer()

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "get provider returns engine" {
            client.getProvider() shouldBe "engine"
        }

        "validate remote succeeds with only required properties" {
            val result = client.validateRemote(mapOf("address" to "host", "username" to "admin", "repository" to "repo"))
            result["address"] shouldBe "host"
            result["username"] shouldBe "admin"
            result["repository"] shouldBe "repo"
        }

        "validate remote succeeds with all properties" {
            val result = client.validateRemote(mapOf("address" to "host", "username" to "admin", "repository" to "repo",
                    "password" to "password"))
            result["address"] shouldBe "host"
            result["username"] shouldBe "admin"
            result["repository"] shouldBe "repo"
            result["password"] shouldBe "password"
        }

        "validate remote fails with missing required property" {
            shouldThrow<IllegalArgumentException> {
                client.validateRemote(mapOf("address" to "host", "username" to "admin"))
            }
        }

        "validate remote fails with invalid property" {
            shouldThrow<IllegalArgumentException> {
                client.validateRemote(mapOf("address" to "host", "username" to "admin", "repository" to "repo", "foo" to "bar"))
            }
        }

        "validate parameters succeeds with empty properties" {
            val result = client.validateParameters(emptyMap())
            result.size shouldBe 0
        }

        "validate parameters succeeds with password" {
            val result = client.validateParameters(mapOf("password" to "password"))
            result["password"] shouldBe "password"
        }

        "validate parameters fails with invalid property" {
            shouldThrow<IllegalArgumentException> {
                client.validateRemote(mapOf("foo" to "bar"))
            }
        }
    }
}
