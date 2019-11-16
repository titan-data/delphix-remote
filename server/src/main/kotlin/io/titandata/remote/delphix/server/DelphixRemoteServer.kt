package io.titandata.remote.delphix.server

import com.delphix.sdk.Delphix
import com.delphix.sdk.Http
import com.delphix.sdk.objects.Repository
import io.titandata.remote.RemoteOperation
import io.titandata.remote.RemoteServer
import io.titandata.remote.RemoteServerUtil
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/**
 * The Delphix remote provider is a proof of concept implementation, dependent on a proprietary plugin that is not
 * currently generally available. As such, this is only loosely documented and tested.
 */
class DelphixRemoteServer : RemoteServer {

    internal val util = RemoteServerUtil()

    companion object {
        val log = LoggerFactory.getLogger(DelphixRemoteServer::class.java)
    }

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

    private fun connect(remote: Map<String, Any>, params: Map<String, Any>): Delphix {
        val engine = Delphix(Http("http://${remote["address"]}"))
        if (remote["password"] == null && params["password"] == null) {
            throw IllegalArgumentException("missing password in remote parameters")
        }
        engine.login(remote["username"] as String, (params["password"] ?: remote["password"]).toString())
        return engine
    }

    private fun findInResult(result: JSONObject, lambda: (JSONObject) -> Boolean): JSONObject? {
        val objects = result.getJSONArray("result")
        for (i in 0 until objects.length()) {
            val obj = objects.getJSONObject(i)
            if (lambda(obj)) {
                return obj
            }
        }
        return null
    }

    private fun findByName(result: JSONObject, name: String): JSONObject? {
        return findInResult(result) { it.getString("name") == name }
    }

    private fun findInGroup(engine: Delphix, groupName: String, name: String): JSONObject? {
        val group = findByName(engine.group().list(), groupName) ?: return null
        val groupRef = group.getString("reference")

        return findInResult(engine.container().list()) {
            it.getString("name") == name && it.getString("group") == groupRef
        }
    }

    private fun getRepository(engine: Delphix): Repository? {
        for (r in engine.repository().list()) {
            if (r.name == "Titan") {
                return r
            }
        }
        return null
    }

    private fun getEnvUser(engine: Delphix, env: JSONObject): JSONObject? {
        return findInResult(engine.environmentUser().list()) {
            it.getString("environment") == env.getString("reference")
        }
    }

    fun waitForJob(engine: Delphix, result: JSONObject) {
        val actionResult: JSONObject = engine.action().read(result.getString("action")).getJSONObject("result")
        if (actionResult.optString("state") == "COMPLETED") {
            log.debug("action ${actionResult.getString("reference")} complete")
            println(actionResult.getString("title"))
        } else {
            log.debug("waiting for job ${result.getString("job")}")
            var job: JSONObject = engine.job().read(result.getString("job")).getJSONObject("result")
            while (job.optString("jobState") == "RUNNING") {
                Thread.sleep(5000)
                job = engine.job().read(result.getString("job")).getJSONObject("result")
            }

            if (job.optString("jobState") != "COMPLETED") {
                throw Exception("engine job ${job.getString("reference")} failed")
            }
            log.debug("engine job ${job.getString("reference")} complete")
        }
    }

    fun repoExists(engine: Delphix, name: String): Boolean {
        return findInGroup(engine, "repositories", name) != null
    }

    private fun listSnapshots(engine: Delphix, remote: Map<String, Any>): List<JSONObject> {
        val name = remote["repository"] as String

        if (!repoExists(engine, name)) {
            return emptyList()
        }

        val ret = ArrayList<JSONObject>()
        val snapshots = engine.snapshot().list().getJSONArray("result")
        for (i in 0 until snapshots.length()) {
            val snapshot = snapshots.getJSONObject(i)
            val properties = snapshot.optJSONObject("metadata")
            if (properties != null && properties.has("repository") && properties.has("hash")) {
                if (properties.getString("repository") == name && properties.getString("hash") != "") {
                    ret.add(properties)
                }
            }
        }

        return ret
    }

    override fun getCommit(remote: Map<String, Any>, parameters: Map<String, Any>, commitId: String): Map<String, Any>? {
        // This is horribly inefficient, but all we can do until we have a better API
        val commits = listCommits(remote, parameters, emptyList())
        return commits.find { it.first == commitId } ?.second
    }

    override fun listCommits(remote: Map<String, Any>, parameters: Map<String, Any>, tags: List<Pair<String, String?>>): List<Pair<String, Map<String, Any>>> {
        val engine = connect(remote, parameters)
        val commits = listSnapshots(engine, remote).map {
            it.getString("hash") to it.getJSONObject("metadata").toMap()
        }
        val filtered = commits.filter { util.matchTags(it.second, tags) }
        return util.sortDescending(filtered)
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
