# Bank Transaction Processor Demo / Testing

A stateful Apache Flink application that processes real-time bank transactions, 
tracks account balances, and flags overdrafts. This is meant to be self-contained (No Kafka or DB for
sources and sinks). **This is built on Flink 1.20**

## 🚀 Features
* **Stateful Balance Tracking:** Uses Flink's `ValueState` Utilizes Flink's ValueState to maintain a distributed "source of truth" for account balances, ensuring sub-millisecond lookups.
* **Overdraft Detection:** Automatically flags transactions that result in a negative balance.
* **High-Value Alerts:** Identifies transactions above a configurable threshold.
* **Checkpointing:** Fault-tolerant state that survives job restarts.
* **Exactly-Once Guarantees:** Configured with Checkpointing to ensure that even if the cluster crashes, account balances remain accurate and no data is double-counted. Currently every 1 min, but if this was real you should checkpoint more often.
* **Self-Contained Testing:** Includes a built-in SourceFunction that generates mock banking traffic, making it 100% runnable out of the box.
* **Real-time Filtering**: The pipeline automatically bifurcates the data stream using a side-logic filter. It classifies transactions into `Standard`, `HighAmountTransaction`, or `OVERDRAFT_WARNING`.

## 📦 Quick Start: Run the JAR

If you want to skip the build process and run the application directly on Flink or Ververica Cloud, you can download the pre-compiled shaded JAR from the Releases page.

1. **Download the latest JAR:** Go to [Releases](https://github.com/mitchellg3/flink-transaction-processor/releases) and download the file named `flink-transaction-processor-latest.jar`.

## 🛠️ Setup & Running
1. **Prerequisites:** Java 21, Maven, and an IDE (IntelliJ recommended).

2. **Clone the repo:** `git clone https://github.com/mitchellg3/flink-transaction-processor.git
   cd flink-transaction-processor`

3. **Run it:** `./run` (or `./run dev`) builds both modules and launches the web control service — see [Run script](#-run-script) below. Or run `mvn clean install` yourself if you'd rather not use it.

4. **Execute directly (optional):** Run the `com.example.flink.job.JobRunner` class from your IDE, exactly as before — this module's behavior is unchanged.

## 🏃 Run script

`./run` (Python under the hood, `run.py`) wraps the Maven commands below so you don't have to remember them:

| Command | What it does |
| :--- | :--- |
| `./run` / `./run dev` | Build both modules, launch the web control service at http://localhost:8080. Add `--port 8081` to use a different port. |
| `./run build` | `mvn clean install` for the whole reactor. |
| `./run test` | `mvn test` for the whole reactor. |
| `./run package` | Build the standalone shaded job jar into `dist/`. |
| `./run clean` | `mvn clean`. |
| `./run doctor` | Check Java/Maven prerequisites; auto-fixes the Maven plugin-group and `JAVA_HOME` issues below. |

It also works around two Maven setup issues in this repo:
- `mvn spring-boot:run` fails with `No plugin found for prefix 'spring-boot'` unless `org.springframework.boot` is registered as a plugin group in `~/.m2/settings.xml` (Maven only auto-searches `org.apache.maven.plugins` / `org.codehaus.mojo` by default). `./run doctor` adds it for you.
- If your system only has a JRE (no `javac`), `./run` looks for a bundled JDK (e.g. an IDE's `~/.jdks/*` or the VS Code Java extension's) and uses it via `JAVA_HOME` for the Maven calls it makes.

Note: `mvn -pl control-service -am spring-boot:run` (a natural-looking manual invocation) does **not** work in this repo — a bare `plugin:goal` on the CLI runs against every project in the resolved reactor, including the parent `pom`-packaged project, which has no main class. `./run dev` instead installs `job` first, then runs `spring-boot:run` scoped to just `control-service`.

## 🧩 Project Structure

This is a two-module Maven reactor:

| Module | What it is |
| :--- | :--- |
| [`job/`](job) | The Flink pipeline itself (unchanged logic): source, stateful balance tracker, webhook/stdout sinks. Builds the standalone shaded jar described above. |
| [`control-service/`](control-service) | A Spring Boot app that embeds a Flink local MiniCluster and serves a **web GUI** for viewing live data, configuring, starting/stopping the job, and basic analysis — see below. |

## 🖥️ Web Control Service

Instead of launching `JobRunner` yourself, you can run the whole thing (Flink job + dashboard) as one process:

```
./run dev
```

(See [Run script](#-run-script) for why this isn't a plain `mvn ... spring-boot:run` invocation.) Then open **http://localhost:8080** for:
- **Live Feed** — real-time transaction stream over WebSocket.
- **Analysis** — throughput, overdraft rate, high-value counts, top accounts by balance.
- **Configuration** — set `maxAccounts`, `checkpointIntervalMs`, `stateBloat`, and the overdraft webhook before starting.
- **Start / Stop** — controls the embedded job's lifecycle without restarting the process.

The job runs in-process (no external Flink cluster needed) and all live data is in-memory only — nothing persists across restarts of the control service.

## 🏗️ Architecture & Logic Flow
The pipeline follows this structure:
`Source -> KeyBy(AccountId) -> Map(Stateful Logic) -> Sink(Print)`

**Ingestion:** Mock transactions (Account ID, Amount, Timestamp) are generated.

**Partitioning (keyBy):** Data is routed by AccountId. All transactions for a specific user are guaranteed to be processed by the same parallel worker.

**Stateful Processing:** A RichMapFunction retrieves the current balance from Flink’s managed memory, applies the transaction, checks for overdrafts, and updates the state.

**Egress:** Results are streamed to the console (standard out) for real-time monitoring.

## ⚙️ Configuration Parameters

The application uses Flink's `ParameterTool` to allow dynamic configuration. You can pass these arguments via the command line or the "Main Arguments" section in Ververica Cloud.

### 📊 Runtime Arguments

| Parameter | Type      | Default | Optional | Description                                                                                                              |
| :--- |:----------|:--------|:---------|:-------------------------------------------------------------------------------------------------------------------------|
| `--max.accounts` | `int`     | `4000`  | ✅    | **Key Cardinality:** Controls the number of unique account IDs. Higher values increase the "width" of the Managed State. |
| `--checkpoint.interval` | `long`    | `60000` | ✅   | **Fault Tolerance:** Interval (in ms) between state snapshots. (e.g., `30000` for 30s).                                  |
| `--state.bloat` | `boolean` | `false` | ✅    | **Increase State Size:** Every Transaction object stored in Flink's ValueState is padded with a 2KB string of junk data                                                                                                |
| `--webhook.enabled` | `boolean` | `false` | ✅    | **Overdraft Alerts:** When `true`, POSTs each `OVERDRAFT_WARNING` transaction as JSON to `webhook.url`.                  |
| `--webhook.url` | `String`  | (webhook.site test URL) | ✅ | Destination URL for overdraft alerts when `webhook.enabled` is `true`.                                                   |

---
## 📅 Roadmap / To-Do
1. Remove warnings and deprecated code
2. Clean up and add better comments
