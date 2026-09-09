plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

group = "co.screenmate.can"
version = "0.1.0"

android {
    namespace = "co.screenmate.can.privileged"
    compileSdk = 34
    defaultConfig { minSdk = 29 }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}

dependencies {
    api(project(":common")) // Kind: value-kind tags on the VendorSignals catalog (public API)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")
}
