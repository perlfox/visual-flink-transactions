#!/usr/bin/env python3
"""
Run framework for flink-transaction-processor.

Wraps the Maven commands you'd otherwise have to remember/type by hand.
Usage:
    ./run.py dev          # build job+control-service, launch the web control service (default)
    ./run.py build        # mvn clean install (whole reactor)
    ./run.py test         # mvn test (whole reactor)
    ./run.py package      # build the standalone job shaded jar -> dist/
    ./run.py clean        # mvn clean
    ./run.py doctor       # check Java/Maven/plugin-group prerequisites
"""

import argparse
import glob
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent
M2_SETTINGS = Path.home() / ".m2" / "settings.xml"
MIN_JAVA_VERSION = 21

# Candidate locations for a full JDK (with javac) when the system only has a JRE.
# openjdk-21-jre-headless (no javac) is common on dev boxes where the JDK only
# ships bundled inside an IDE. Globs are checked newest-looking-version first.
JDK_SEARCH_GLOBS = [
    str(Path.home() / ".jdks" / "*"),
    str(Path.home() / ".vscode" / "extensions" / "redhat.java-*" / "jre" / "*"),
    str(Path.home() / ".vscode-server" / "extensions" / "redhat.java-*" / "jre" / "*"),
    "/usr/lib/jvm/*",
]


def find_jdk_home():
    """Return a JAVA_HOME whose bin/javac exists, or None if the active `java` already has one."""
    if shutil.which("javac"):
        return None  # system PATH already has a full JDK; nothing to override

    candidates = []
    for pattern in JDK_SEARCH_GLOBS:
        for path in glob.glob(pattern):
            javac = Path(path) / "bin" / "javac"
            if javac.is_file() and os.access(javac, os.X_OK):
                candidates.append(Path(path))
    # Prefer one whose version string contains our target release (e.g. "21").
    candidates.sort(key=lambda p: (str(MIN_JAVA_VERSION) not in p.name, str(p)))
    return candidates[0] if candidates else None


def run(cmd, cwd=ROOT, env=None):
    print(f"\033[36m$ {' '.join(cmd)}\033[0m")
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode != 0:
        sys.exit(result.returncode)


def mvn_env():
    """Build the environment for mvn subprocess calls, auto-injecting JAVA_HOME if needed."""
    env = os.environ.copy()
    if env.get("JAVA_HOME"):
        return env
    jdk_home = find_jdk_home()
    if jdk_home:
        print(f"\033[33mNo javac on PATH; using JAVA_HOME={jdk_home}\033[0m")
        env["JAVA_HOME"] = str(jdk_home)
        env["PATH"] = f"{jdk_home / 'bin'}{os.pathsep}{env.get('PATH', '')}"
    return env


def require(binary):
    if shutil.which(binary) is None:
        print(f"\033[31mError: '{binary}' not found on PATH.\033[0m")
        sys.exit(1)


def check_java_version():
    result = subprocess.run(["java", "-version"], capture_output=True, text=True)
    output = result.stdout + result.stderr
    first_line = output.splitlines()[0] if output else ""
    digits = "".join(c for c in first_line.split('"')[1].split(".")[0] if c.isdigit()) \
        if '"' in first_line else ""
    major = int(digits) if digits else 0
    if major < MIN_JAVA_VERSION:
        print(f"\033[33mWarning: Java {major or '?'} detected, this project targets Java {MIN_JAVA_VERSION}.\033[0m")
    else:
        print(f"\033[32mJava {major} OK\033[0m")


def ensure_spring_boot_plugin_group():
    """
    'mvn spring-boot:run' fails with 'No plugin found for prefix' unless
    org.springframework.boot is registered as a plugin group in settings.xml
    (Maven only auto-searches org.apache.maven.plugins / org.codehaus.mojo by default).
    This makes that fix durable instead of a one-off manual edit.
    """
    group = "org.springframework.boot"
    M2_SETTINGS.parent.mkdir(parents=True, exist_ok=True)

    if not M2_SETTINGS.exists():
        M2_SETTINGS.write_text(
            "<settings xmlns=\"http://maven.apache.org/SETTINGS/1.0.0\">\n"
            "  <pluginGroups>\n"
            f"    <pluginGroup>{group}</pluginGroup>\n"
            "  </pluginGroups>\n"
            "</settings>\n"
        )
        print(f"\033[32mCreated {M2_SETTINGS} with pluginGroup {group}\033[0m")
        return

    text = M2_SETTINGS.read_text()
    if group in text:
        print(f"\033[32mplugin group '{group}' already registered\033[0m")
        return

    try:
        ns = "http://maven.apache.org/SETTINGS/1.0.0"
        ET.register_namespace("", ns)
        tree = ET.parse(M2_SETTINGS)
        rootEl = tree.getroot()

        def tag(name):
            return f"{{{ns}}}{name}" if rootEl.tag.startswith("{") else name

        plugin_groups = rootEl.find(tag("pluginGroups"))
        if plugin_groups is None:
            plugin_groups = ET.SubElement(rootEl, tag("pluginGroups"))
        pg = ET.SubElement(plugin_groups, tag("pluginGroup"))
        pg.text = group
        tree.write(M2_SETTINGS, xml_declaration=True, encoding="UTF-8")
        print(f"\033[32mAdded pluginGroup {group} to {M2_SETTINGS}\033[0m")
    except ET.ParseError as e:
        print(f"\033[31mCould not auto-edit {M2_SETTINGS} ({e}); "
              f"add <pluginGroup>{group}</pluginGroup> under <pluginGroups> yourself.\033[0m")


def cmd_doctor(_args):
    require("mvn")
    require("java")
    check_java_version()
    ensure_spring_boot_plugin_group()
    if not shutil.which("javac"):
        jdk_home = find_jdk_home()
        if jdk_home:
            print(f"\033[33mNo javac on PATH; will use JAVA_HOME={jdk_home} for mvn builds\033[0m")
        else:
            print("\033[31mNo javac found anywhere (system JRE-only and no bundled IDE JDK detected).\033[0m")
            print("\033[31mInstall a JDK, e.g.: sudo apt install openjdk-21-jdk\033[0m")


def cmd_build(_args):
    cmd_doctor(_args)
    run(["mvn", "clean", "install"], env=mvn_env())


def cmd_test(_args):
    run(["mvn", "test"], env=mvn_env())


def cmd_clean(_args):
    run(["mvn", "clean"], env=mvn_env())


def port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def cmd_dev(args):
    """
    Build both modules, then launch the Spring Boot control service (embedded Flink job + web UI).

    Deliberately NOT `mvn -pl control-service -am spring-boot:run`: a bare plugin:goal (not a
    lifecycle phase) runs against every project in the resolved reactor, including this repo's
    root aggregator pom, which has no main class and blows up before reaching control-service.
    So: install `job` into the local repo first (one full reactor pass), then invoke
    spring-boot:run scoped to just the control-service module.
    """
    cmd_doctor(args)
    port = args.port
    if port_in_use(port):
        print(f"\033[31mPort {port} is already in use by another process on this machine.\033[0m")
        print(f"\033[31mFree it, or pick another: ./run dev --port 8081\033[0m")
        sys.exit(1)

    env = mvn_env()
    run(["mvn", "-q", "install", "-pl", "job", "-am", "-DskipTests"], env=env)
    print(f"\033[32mStarting control service on http://localhost:{port} (Ctrl+C to stop)\033[0m")
    mvn_args = ["mvn", "spring-boot:run"]
    if port != 8080:
        mvn_args.append(f"-Dspring-boot.run.arguments=--server.port={port}")
    run(mvn_args, cwd=ROOT / "control-service", env=env)


def cmd_package(_args):
    """Build the standalone shaded job jar and copy it to dist/."""
    run(["mvn", "-pl", "job", "-am", "package"], env=mvn_env())
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    built = list((ROOT / "job" / "target").glob("*.jar"))
    shaded = [j for j in built if "original" not in j.name]
    if not shaded:
        print("\033[31mNo jar found in job/target after package.\033[0m")
        sys.exit(1)
    for jar in shaded:
        dest = dist / jar.name
        shutil.copy2(jar, dest)
        print(f"\033[32mCopied {jar} -> {dest}\033[0m")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command")

    dev_parser = sub.add_parser("dev", help="Build + run the web control service (default)")
    dev_parser.add_argument("--port", type=int, default=8080, help="Port for the control service (default: 8080)")
    dev_parser.set_defaults(func=cmd_dev)
    sub.add_parser("build", help="mvn clean install (whole reactor)").set_defaults(func=cmd_build)
    sub.add_parser("test", help="mvn test (whole reactor)").set_defaults(func=cmd_test)
    sub.add_parser("package", help="Build job's shaded jar into dist/").set_defaults(func=cmd_package)
    sub.add_parser("clean", help="mvn clean").set_defaults(func=cmd_clean)
    sub.add_parser("doctor", help="Check Java/Maven prerequisites and fix plugin-group settings").set_defaults(func=cmd_doctor)

    args = parser.parse_args()
    if not args.command:
        args = dev_parser.parse_args([])
        cmd_dev(args)
        return
    args.func(args)


if __name__ == "__main__":
    main()
