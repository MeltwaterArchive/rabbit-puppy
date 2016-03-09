package com.meltwater.puppy.config

data class PermissionsData(var configure: String = ".*",
                           var write: String = ".*",
                           var read: String = ".*")