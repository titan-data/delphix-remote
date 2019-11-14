/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Super schema for all schemas representing user-visible objects that have a read-only &#39;name&#39;.
 */
interface ReadonlyNamedUserObject : UserObject {
    override val name: String? // Object name.
    override val reference: String? // The object reference.
    override val namespace: String? // Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
