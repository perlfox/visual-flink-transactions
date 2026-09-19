# Bank Transaction Processor Demo / Testing

A stateful Apache Flink application that processes real-time bank transactions,
tracks account balances, and flags overdrafts. This is meant to be self-contained (No Kafka or DB for
sources and sinks). **This is built on Flink 1.20**

## 🚀 Features
* **Stateful Balance Tracking:** Uses Flink's `ValueState` to maintain a distributed "source of truth" for account balances, ensuring sub-millisecond lookups.
* **Overdraft Detection:** Automatically flags transactions that result in a negative balance.
* **High-Value Alerts:** Identifies transactions above a configurable threshold.
* **Checkpointing:** Fault-tolerant state that survives job restarts, with configurable interval and mode (exactly-once / at-least-once).
* **Configurable Restart Strategy:** Fixed-delay restarts on failure, with attempts and delay both configurable (or disabled entirely).
* **Self-Contained Testing:** Includes a built-in SourceFunction that generates mock banking traffic, making it 100% runnable out of the box.
* **Real-time Filtering**: The pipeline automatically bifurcates the data stream using a side-logic filter. It classifies transactions into `Standard`, `HighAmountTransaction`, or `OVERDRAFT_WARNING`.
* **Web Control Service:** A Spring Boot app that embeds a Flink local MiniCluster and serves a dashboard for live data, configuration, and job lifecycle control — see below.

## 🛠️ Setup & Running

1. **Clone the repo:**
   ```
   git clone https://github.com/mitchellg3/flink-transaction-processor.git
   cd flink-transaction-processor
   ```

2. **New machine? Install dependencies first:** `./setup.sh` installs everything this project needs (git, a JDK 21, Maven, Python 3) via your OS package manager, then runs `./run doctor` to finish Maven setup. Pass `-y` to skip prompts (e.g. for a fresh VM or container: `./setup.sh -y`). If you already have Java 21, Maven, and Python 3 on PATH, you can skip this step.

3. **Run it:** `./run` (or `./run dev`) builds both modules and launches the web control service — see [Run script](#-run-script) below.

## 🏃 Run script

`./run` (Python under the hood, `run.py`) wraps the Maven commands below so you don't have to remember them:

| Command | What it does |
| :--- | :--- |
| `./run` / `./run dev` | Build both modules, launch the web control service at http://localhost:8080. Add `--port 8081` to use a different port. |
| `./run build` | `mvn clean install` for the whole reactor. |
| `./run test` | `mvn test` for the whole reactor. |
| `./run package` | Build the standalone shaded job jar into `dist/`, for running the pipeline outside this repo (e.g. on Ververica Cloud or a real Flink cluster). |
| `./run clean` | `mvn clean`. |
| `./run doctor` | Check Java/Maven prerequisites; auto-fixes the Maven plugin-group and `JAVA_HOME` issues below. |

`./run dev` and `./run build` run `./run doctor` first automatically.

It also works around two Maven setup issues in this repo:
- `mvn spring-boot:run` fails with `No plugin found for prefix 'spring-boot'` unless `org.springframework.boot` is registered as a plugin group in `~/.m2/settings.xml` (Maven only auto-searches `org.apache.maven.plugins` / `org.codehaus.mojo` by default). `./run doctor` adds it for you.
- If your system only has a JRE (no `javac`), `./run` looks for a bundled JDK (e.g. an IDE's `~/.jdks/*` or the VS Code Java extension's) and uses it via `JAVA_HOME` for the Maven calls it makes.

Note: `mvn -pl control-service -am spring-boot:run` (a natural-looking manual invocation) does **not** work in this repo — a bare `plugin:goal` on the CLI runs against every project in the resolved reactor, including the parent `pom`-packaged project, which has no main class. `./run dev` instead installs `job` first, then runs `spring-boot:run` scoped to just `control-service`.

## 📦 Setup script (`setup.sh`)

For a brand-new machine (bare VM, fresh container, CI runner) that doesn't have the toolchain yet, `./setup.sh`:
- Detects the OS package manager (`apt`, `dnf`, `yum`, `pacman`, or Homebrew).
- Installs `git`, a JDK 21 (with `javac`, not just a JRE), Maven, and Python 3 — whichever of these are missing.
- Runs `./run doctor` afterward to register the Spring Boot Maven plugin group and confirm `JAVA_HOME`.

It's plain bash with no dependencies of its own (unlike `run.py`, which needs Python 3 already installed), so it's safe to run as the very first command after cloning. Use `./setup.sh -y` to install without interactive prompts.

## 🧩 Project Structure

This is a two-module Maven reactor:

| Module | What it is |
| :--- | :--- |
| [`job/`](job) | The Flink pipeline itself: source, stateful balance tracker, webhook/stdout sinks. Can also be run standalone (e.g. on a real Flink cluster) via `./run package`, which builds its shaded jar into `dist/`. |
| [`control-service/`](control-service) | A Spring Boot app that embeds a Flink local MiniCluster and serves a **web GUI** for viewing live data, configuring, starting/stopping the job, and basic analysis — see below. |

## 🖥️ Web Control Service

Run the whole thing (Flink job + dashboard) as one process:

```
./run dev
```

(See [Run script](#-run-script) for why this isn't a plain `mvn ... spring-boot:run` invocation.) Then open **http://localhost:8080** for:

- **Live Feed** — real-time transaction stream over WebSocket.
- **Analysis** — partitioning info (keyBy field, parallelism, key space), throughput, overdraft rate, high-value counts, top accounts by balance.
- **Configuration** — `maxAccounts`, state bloat toggle, and the overdraft webhook (enable + URL).
- **Flink Config** — runtime environment info (Flink version, execution mode, state backend, active parallelism), and the execution/memory settings below:
  - Parallelism, checkpoint interval, checkpointing mode (exactly-once / at-least-once), restart attempts, restart delay
  - Max transactions/sec and task-manager memory cap (both clamped server-side so the embedded MiniCluster, which shares this JVM's heap, can't run the host out of memory)
  - **Save Configuration** — persists these settings server-side without starting a job, so they survive even if you don't click Start right away. (Disabled while a job is running/transitioning — stop the job first to change them.)
- **Start / Stop** — controls the embedded job's lifecycle without restarting the process. Start uses whatever is currently in the Configuration/Flink Config forms (clamped to safe ranges); values submitted outside the shown min/max are silently clamped rather than rejected.
- **Clear Data** — resets the live feed, aggregates, and (if the job is running) account balances, without a full restart of the process.

The job runs in-process (no external Flink cluster needed) and all live data is in-memory only — nothing persists across restarts of the control service.

## 🏗️ Architecture & Logic Flow
The pipeline follows this structure:
`Source -> KeyBy(AccountId) -> Map(Stateful Logic) -> Sink(Print / Webhook)`

**Ingestion:** Mock transactions (Account ID, Amount, Timestamp) are generated.

**Partitioning (keyBy):** Data is routed by AccountId. All transactions for a specific user are guaranteed to be processed by the same parallel worker.

**Stateful Processing:** A RichMapFunction retrieves the current balance from Flink's managed memory, applies the transaction, checks for overdrafts, and updates the state.

**Egress:** Results are streamed to the console (standard out) for real-time monitoring, and optionally POSTed to a webhook for overdraft alerts.

## ⚙️ Standalone Job Parameters

If you build and run the `job` module's shaded jar directly (`./run package`, then `java -jar dist/*.jar ...`) instead of using the web control service, it uses Flink's `ParameterTool` to read these command-line arguments:

### 📊 Runtime Arguments

| Parameter | Type      | Default | Optional | Description                                                                                                              |
| :--- |:----------|:--------|:---------|:-------------------------------------------------------------------------------------------------------------------------|
| `--max.accounts` | `int`     | `4000`  | ✅    | **Key Cardinality:** Controls the number of unique account IDs. Higher values increase the "width" of the Managed State. |
| `--checkpoint.interval` | `long`    | `60000` | ✅   | **Fault Tolerance:** Interval (in ms) between state snapshots. (e.g., `30000` for 30s).                                  |
| `--state.bloat` | `boolean` | `false` | ✅    | **Increase State Size:** Every Transaction object stored in Flink's ValueState is padded with a 2KB string of junk data                                                                                                |
| `--webhook.enabled` | `boolean` | `false` | ✅    | **Overdraft Alerts:** When `true`, POSTs each `OVERDRAFT_WARNING` transaction as JSON to `webhook.url`.                  |
| `--webhook.url` | `String`  | (webhook.site test URL) | ✅ | Destination URL for overdraft alerts when `webhook.enabled` is `true`.                                                   |
| `--rate.limit` | `int`  | `1000` | ✅ | Caps the mock source's output rate in transactions/sec.                                                   |

The web control service's Configuration/Flink Config tabs cover the same knobs (plus parallelism, checkpointing mode, restart strategy, and task-manager memory) without needing command-line args.

---
## 📅 Roadmap / To-Do
1. Remove warnings and deprecated code
2. Clean up and add better comments
