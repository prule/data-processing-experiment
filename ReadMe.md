Creating the gradle project
====

To get started I used `gradle init` to create a kotlin application - I'll want a command line application as the entry point and from here I'll add subprojects to this to structure the different parts of the system. If this was a real project I'd probably put most of these subprojects into their own repositories, so they could each have their own release cycle - but for speed and simplicity for now I'll keep them together. This decision is easily changeable so it's not worth dwelling on at the moment.

```
% gradle init \
  --type kotlin-application \
  --dsl kotlin \
  --test-framework kotlintest \
  --package com.example.dataprocessingexperiment.app \
  --project-name data-processing-experiment  \
  --no-split-project  \
  --java-version 17

Generate build using new APIs and behavior (some features may change in the next minor release)? (default: no) [yes, no] 


> Task :init
To learn more about Gradle by exploring our Samples at https://docs.gradle.org/8.6/samples/sample_building_kotlin_applications.html

BUILD SUCCESSFUL in 2s
1 actionable task: 1 executed
```

Now to add a subproject for the spark code I need a subfolder and a copy of the app build file which I can modify for the subproject...

```
mkdir spark
cp app/build.gradle.kts spark
```

Now `spark/build.gradle.kts` just needs editing to make it appropriate for the subproject

- remove the application plugin
- add some spark dependencies
- add some JVM args so spark plays nice with Java 17

In `app/build.gradle.kts` we'll add those same JVM args for when we run the application, and make `app` depend on `spark`


I'm using java 17 here, for no particular reason other than it's well established.

```
% gradle --version

------------------------------------------------------------
Gradle 8.6
------------------------------------------------------------

Build time:   2024-02-02 16:47:16 UTC
Revision:     d55c486870a0dc6f6278f53d21381396d0741c6e

Kotlin:       1.9.20
Groovy:       3.0.17
Ant:          Apache Ant(TM) version 1.10.13 compiled on January 4 2023
JVM:          17.0.7 (Eclipse Adoptium 17.0.7+7)
OS:           Mac OS X 14.2.1 aarch64
```

Set up git:
```
% git init
% git remote add origin https://github.com/prule/data-processing-experiment.git
```

Run the application to make sure everything is hanging together:
```
% ./gradlew app:run         

> Task :app:run
Hello World!
Hello Spark

BUILD SUCCESSFUL in 1s
6 actionable tasks: 6 executed
```

Run tests:
```
% ./gradlew clean test

BUILD SUCCESSFUL in 1s
11 actionable tasks: 11 executed
```

View the test reports at:
- ./app/build/reports/tests/test/index.html
- ./spark/build/reports/tests/test/index.html

Okay, so now the basic project is set up, I can get started!