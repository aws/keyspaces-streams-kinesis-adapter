## Keyspaces Streams Kinesis Adapter for Java
Keyspaces Streams Kinesis Adapter implements the Amazon Kinesis interface so that your application can use Amazon Kinesis Client Library \(KCL\) to consume and process change events from a Keyspaces Table Stream. You can get started in minutes using *Maven*.

• [Amazon Keyspaces Details][1]

• [Keyspaces Change Data Capture Details][2]

• [Keyspaces Change Data Capture Developer Guide][3]

• [Amazon Kinesis Client Library GitHub and Documentation][4]

• [Issues][5]

## Features
• The Keyspaces Streams Kinesis Adapter for KCL is the best way to ingest and process change events from tables.

• KCL is designed to process streams from Amazon Kinesis and using this Keyspaces Streams Kinesis Adapter, your application can process Streams from Amazon Keyspaces instead.

## Getting Started
• **Sign up for AWS** - Before you begin, you need an AWS account. Please see the [AWS Account and Credentials][6] section of the developer guide for information about how to create an AWS account and retrieve your AWS credentials.

• **Setup access to Amazon Keyspaces** - Please review [Amazon Keyspaces Access Setup][7] to gain access to Amazon Keyspaces as well as information about creation and management of Keyspaces and Tables.

• **Enable Change Data Capture** - Please review [Keyspaces Change Data Capture Developer Guide][3] to enable change data capture on your table that creates the Stream that provides your application with change events.

• **Minimum requirements** - To utilize this library, your application will need to be on Java 8+. Additionally, your maven version for builds should be 3.0.0+.

• **Install the Keyspaces Streams Kinesis Adapter** - You can build and install Keyspaces Streams Kinesis Adapter to use in your application by following the guidelines in "Building From Source" section.

• **Build your first application** - Please review [Keyspaces Streams Kinesis Adapter Details][8] and [Keyspaces Streams Kinesis Adapter Developer Guide][9] that will help you build first application using this adapter.

## Building From Source

• Clone (git clone) the repository from: https://github.com/aws/keyspaces-streams-kinesis-adapter

• Perform a build from source using *Maven*: `mvn clean install`. The jar will be generated under `<GIT_CLONE_PATH>/target` directory. Additionally, this will also install it to your local Maven repository (usually located at `~/.m2/repository`).

• You can now add the following dependency in your target application:

```
    <dependency>
        <groupId>software.amazon.keyspaces</groupId>
        <artifactId>keyspaces-streams-kinesis-adapter</artifactId>
        <version>1.0.0</version>
    </dependency>
```

[1]: https://docs.aws.amazon.com/keyspaces/latest/devguide/what-is-keyspaces.html
[2]: https://docs.aws.amazon.com/keyspaces/latest/devguide/cdc_how-it-works.html
[3]: https://docs.aws.amazon.com/keyspaces/latest/devguide/cdc_how-to-use.html
[4]: https://github.com/awslabs/amazon-kinesis-client
[5]: https://github.com/aws/keyspaces-streams-kinesis-adapter/issues
[6]: https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-creating.html
[7]: https://docs.aws.amazon.com/keyspaces/latest/devguide/getting-started.html
[8]: https://docs.aws.amazon.com/keyspaces/latest/devguide/cdc_how-to-use-kcl.html
[9]: https://docs.aws.amazon.com/keyspaces/latest/devguide/cdc-kcl-implementation.html 
