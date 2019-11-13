/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.delphix.client

import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import java.io.Console
import java.net.URI

class DelphixRemoteClientTest : StringSpec() {

    @MockK
    lateinit var console: Console

    @InjectMockKs
    @OverrideMockKs
    var client = DelphixRemoteClient()

    override fun beforeTest(testCase: TestCase) {
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "parsing full engine URI succeeds" {
            val result = client.parseUri(URI("engine://user:pass@host/path"), emptyMap())
            result["username"] shouldBe "user"
            result["password"] shouldBe "pass"
            result["address"] shouldBe "host"
            result["repository"] shouldBe "path"
        }

        "parsing engine URI without password succeeds" {
            val result = client.parseUri(URI("engine://user@host/path"), emptyMap())
            result["username"] shouldBe "user"
            result["password"] shouldBe null
            result["address"] shouldBe "host"
            result["repository"] shouldBe "path"
        }

        "plain engine provider fails" {
            shouldThrow<IllegalArgumentException> {
                client.parseUri(URI("engine"), emptyMap())
            }
        }

        "specifying engine port fails" {
            shouldThrow<IllegalArgumentException> {
                client.parseUri(URI("engine://user:pass@host:123/path"), emptyMap())
            }
        }

        "missing username in engine URI fails" {
            shouldThrow<IllegalArgumentException> {
                client.parseUri(URI("engine://host/path"), emptyMap())
            }
        }

        "missing path in engine URI fails" {
            shouldThrow<IllegalArgumentException> {
                client.parseUri(URI("engine://user@host"), emptyMap())
            }
        }

        "empty path in engine URI fails" {
            shouldThrow<IllegalArgumentException> {
                client.parseUri(URI("engine://user@host/"), emptyMap())
            }
        }

        "missing host in engine URI fails" {
            shouldThrow<IllegalArgumentException> {
                client.parseUri(URI("engine://user@/path"), emptyMap())
            }
        }

        "engine remote to URI succeeds" {
            val (result, props) = client.toUri(mapOf("address" to "host", "username" to "user",
                    "repository" to "foo"))
            result shouldBe "engine://user@host/foo"
            props.size shouldBe 0
        }

        "engine remote with password to URI succeeds" {
            val (result, props) = client.toUri(mapOf("address" to "host", "username" to "user",
                    "repository" to "foo", "password" to "pass"))
            result shouldBe "engine://user:pass@host/foo"
            props.size shouldBe 0
        }

        "get engine parameters succeeds" {
            val result = client.getParameters(mapOf("address" to "host", "username" to "user",
                    "repository" to "foo", "password" to "pass"))
            result["password"] shouldBe null
        }

        "get engine parameters prompts for password" {
            every { console.readPassword(any()) } returns "pass".toCharArray()
            val result = client.getParameters(mapOf("address" to "host", "username" to "user",
                    "repository" to "foo"))
            result["password"] shouldBe "pass"
        }
    }
}
