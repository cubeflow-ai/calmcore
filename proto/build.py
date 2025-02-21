import subprocess
import os
import shutil
import pwd

proto_files = [
    "core.proto",
    "calmserver.proto",
]



def generate_protobuf_fiels(language, output_dir, proto_file):
    docker_command = [
        'docker', 'run', '--rm',
        '-v', "./:/defs",
        'namely/protoc-all:latest',
        '-f', proto_file,
        '-l', language,
        '-o', output_dir,
    ]

    try:
        subprocess.run(docker_command, check=True, cwd="./src")
        print(f"Generated protobuf for {proto_file}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to generate protobuf for {proto_file}: {e}")

for proto_file in proto_files:
    generate_protobuf_fiels("go", "go_pb/src/proto", proto_file)

# generate go files
current_user = pwd.getpwuid(os.getuid()).pw_name
directory = "./src/go_pb"
chown_command = f"sudo chown -R {current_user}:{current_user} {directory}"
subprocess.run(chown_command, shell=True, check=True)

target_dir = "../../processor/calmclient/proto"

shutil.rmtree(target_dir, ignore_errors=True)
shutil.move("./src/go_pb/src/proto/indexer/proto", target_dir)
shutil.rmtree("./src/go_pb", ignore_errors=True)


