"""隔离模式启动器：-I 下 sys.path 不含工作目录，显式注入项目根后运行 CLI。

数据文件是不可信输入，所有读取它们的 Python 都用 -I 运行，避免工作目录下
被植入的 json.py 等模块劫持标准库导入。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from warehouse_data.cli import main  # noqa: E402

if __name__ == "__main__":
    main()
