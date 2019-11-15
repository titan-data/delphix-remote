package io.titandata.remote.delphix.server

import io.titandata.remote.RemoteOperation
import io.titandata.remote.RemoteServer
import io.titandata.remote.RemoteServerUtil

/**
 * The Delphix remote provider is a proof of concept implementation, dependent on a proprietary plugin that is not
 * currently generally available. As such, this is only loosely documented and tested.
 */
class DelphixRemoteServer : RemoteServer {

    internal val util = RemoteServerUtil()

    override fun getProvider(): String {
        return "engine"
    }

    override fun validateRemote(remote: Map<String, Any>): Map<String, Any> {
        util.validateFields(remote, listOf("username", "address", "repository"), listOf("password"))
        return remote
    }

    override fun validateParameters(parameters: Map<String, Any>): Map<String, Any> {
        util.validateFields(parameters, emptyList(), listOf("password"))
        return parameters
    }

    override fun getCommit(remote: Map<String, Any>, parameters: Map<String, Any>, commitId: String): Map<String, Any>? {
        throw NotImplementedError()
    }

    override fun listCommits(remote: Map<String, Any>, parameters: Map<String, Any>, tags: List<Pair<String, String?>>): List<Pair<String, Map<String, Any>>> {
        throw NotImplementedError()
    }

    override fun endOperation(operation: RemoteOperation, isSuccessful: Boolean) {
        throw NotImplementedError()
    }

    override fun startOperation(operation: RemoteOperation) {
        throw NotImplementedError()
    }

    override fun syncVolume(operation: RemoteOperation, volumeName: String, volumeDescription: String, volumePath: String, scratchPath: String) {
        throw NotImplementedError()
    }
}
