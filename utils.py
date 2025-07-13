import os
import zipfile
import shutil
import stat  # 用于处理文件属性
from config import *


def unzip_file(zip_path, output_dir=BACKTESTING_NEW_DIR):
    """
    从 zip 文件中提取所有 Excel 文件
    :param zip_path: str | Path
    :param output_dir: str | Path
    :return: bool
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                # 跳过__MACOSX文件夹
                if file_info.filename.startswith('__MACOSX'):
                    continue

                if file_info.filename.lower().endswith(('.xls', '.xlsx', '.xlsm', '.xlsb', '.cvs')):
                    # 将文件写入目标目录(os.path.basename只保留文件名，省去路径名，路径名有编码问题)
                    output_path = os.path.join(output_dir, os.path.basename(file_info.filename))
                    with open(output_path, 'wb') as f:
                        f.write(zip_ref.read(file_info))

        print("解压成功")
        return True

    except Exception as e:
        print(f"处理文件 {zip_path} 时出错: ", type(e), e)
        return False


def unzip_all_and_backup():
    """
    1.解压：将BACKTESTING_NEW_DIR中所有zip文件中的excel文件解压到该目录
    2.备份：将成功解压的zip文件移动到BACKTESTING_BACKUP_DIR
    """
    # 确保备份目录存在
    BACKTESTING_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    zip_file_list = list(BACKTESTING_NEW_DIR.glob('*.zip'))
    if not zip_file_list:
        print("没有待解压文件")
        return

    # 统计成功解压备份文件数
    count = 0

    for zip_file in zip_file_list:
        print("===== 正在处理：", zip_file.name, "=====")

        if not unzip_file(zip_file): # 解压
            continue

        try:
            # 默认只读，添加写权限
            os.chmod(zip_file, stat.S_IWRITE)

            # 将zip文件移动到backup目录
            shutil.move(zip_file, BACKTESTING_BACKUP_DIR / zip_file.name)
            count += 1
            print("备份成功")

        except Exception as e:
            print(f"移动删除文件{zip_file.name}时异常：", type(e), e)

    print(f"解压备份结束，共成功({count}/{len(zip_file_list)})")


if __name__ == "__main__":
    unzip_all_and_backup()


