import argparse
import sys
import io
from scripts import SCRIPTS
from error import ArgumentError, OperationError


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('script', type=str, choices=list(SCRIPTS.keys()), help='需要运行的脚本')
    parser.add_argument('script_args', type=str, nargs='*')
    parser.add_argument('--database_config', type=str, default='database_config.json', help='数据库配置')
    return parser.parse_args()


def main():
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    args = get_args()

    try:
        database_config, script_args = args.database_config, args.script_args or []
        with SCRIPTS[args.script](database_config=database_config) as script:
            return script(*script_args)
    except OperationError as e:
        print(f'操作错误: {e}', file=sys.stderr)
        return 1
    except ArgumentError as e:
        print(f'参数错误，请参看用户手册：{e}', file=sys.stderr)
        return 2
    except Exception as e:
        print(f'遇到未知错误：{e}')
        raise e


if __name__ == '__main__':
    exit(main())
