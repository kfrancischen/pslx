"""
PYTHONPATH=. python example/frontend_example/create_config.py
"""
from pslx.util.common_util import CommonUtil
from pslx.util.file_util import FileUtil


def main():
    yaml_path = "example/frontend_example/frontend_config.yaml"
    proto_path = "example/frontend_example/frontend_config.pb"
    config = CommonUtil.make_frontend_config(yaml_path=yaml_path)
    print(config)
    FileUtil.write_proto_to_file(proto=config, file_name=proto_path)


if __name__ == "__main__":
    main()
