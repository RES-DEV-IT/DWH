import subprocess
import os
import urllib
import patoolib
import shutil
import platform


def create_dir_powershell(path):
    try:
        subprocess.run(
            ["powershell", "-Command", f"New-Item -ItemType Directory -Force -Path '{path}'"],
            check=True,
            shell=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Ошибка PowerShell: {e}")

def rmtree_cmd(path):
    abs_path = os.path.abspath(path)

    if platform.system() == "Windows":
        if os.path.isdir(abs_path):
            shutil.rmtree(path=fr"\\?\\{abs_path}")
    elif platform.system() in ("Linux", "Darwin"):
        shutil.rmtree(path=path)

def copy_cmd(path_from, path_to, fname):
    if platform.system() == "Windows":
        path_from = fr"\\?\\{os.path.abspath(path_from)}\{fname}"
        path_to = fr"\\?\\{os.path.abspath(path_to)}"
        shutil.copy(path_from, path_to)
        #subprocess.run(f"robocopy '{path_from}' '{path_to}' '{fname}'")
    elif platform.system() in ("Linux", "Darwin"):
        if not os.path.exists(path_to + '/' + fname):
            shutil.copy(path_from + '/' + fname, path_to)

def extract_cmd(arc_path, extract_path):
    subprocess.run([
        r"C:\Program Files\7-Zip\7z.exe",
        "x",
        arc_path,
        f"-o{extract_path}",
        "-aou"
    ], check=True)

def rm_cmd(path):
    if platform.system() == "Windows":
        abs_path = os.path.abspath(path)
        path_to_remove = fr"\\?\\{abs_path}"
        os.remove(path_to_remove)
    else:
        abs_path = os.path.abspath(path)
        if os.path.isfile(abs_path):
            os.remove(abs_path)
        elif os.path.isdir(abs_path):
            os.rmdir(abs_path)
    #subprocess.run(f' {path} -Force', check=True)
    
def remove_undue_folder(src, dst):
    # Перемещаем каждый файл/папку из `111/123` в `111/`
    for item in os.listdir(src):
        src_path = os.path.join(src, item)
        dst_path = os.path.join(dst, item)
        if os.path.exists(dst_path):
            # Если файл/папка уже существует, можно:
            # - перезаписать (осторожно!)
            # - пропустить
            # - добавить суффикс (например, `file_1.txt`)
            print(f"Объект {dst_path} уже существует, пропускаем...")
            continue
        shutil.move(src_path, dst_path)
    
    rm_cmd(src)

def save(record, delete_after=True):
    print("--------------------------------------------")
    print(f"LOCAL SAVING [R-{record.profile['bare']}]")
    print("    > LOCAL_PATH", record.path())

    path = record.path()
    all_filenames = []

    path_on_share = "Z:/Проекты/" + "/".join(path.split("/")[1:])
    print("    > SHARE_PATH", path_on_share)

    if platform.system() == "Windows":
        path_to_create = fr"\\?\\{os.path.abspath(path)}"
        os.makedirs(path_to_create, exist_ok=True)
    else:
        os.makedirs(path, exist_ok=True)

    print("    > DOWNLOADING...")
    for (url, filename, size) in record.iterate_through_files():

        print(f"    | FILENAME = {filename}")
        print(f"    | SIZE = {size / 1024 / 1024:.2f} MB")
        # Скачиваем файл (или архив)
        urllib.request.urlretrieve(url, filename)
    
        # 1. Если архив
        if filename.endswith('.zip') or filename.endswith('.rar'):

            # end_folder_to_extract = filename[:-4]
            
            print("---> Extracting:", filename, f"---> {path + '/'}")

            # Создаем папку, если её не существет
            if not os.path.isdir(path + "/"):
                create_dir_powershell(path + "/")
    
            # Разархивируем
            if platform.system() == "Windows":
                extract_cmd(filename, path + "/")
            elif platform.system() in ("Linux", "Darwin"):
                patoolib.extract_archive(filename, outdir=path + "/", verbosity=-1)
                
            # Удаляем лишнюю папку, если надо
            # if len(os.listdir(path + "/")) == 1 and os.path.isdir(path + "/" + os.listdir(path + "/")[0]): 
            #     undue_folder = os.listdir(path + "/")[0]
            #     print(f"REMOVING UNDUE FOLDER: {undue_folder}")
            #     remove_undue_folder(src=path + "/" + undue_folder, dst=path + "/")
                
            # else:
            #     print(f"NO UNDUE FOLDERS: {os.listdir(path + "/")}")
                # for filename in os.listdir(path + "/" + undue_folder)
            # Проходимся по всему, чтобы разархивировали
            for extracted_filename in os.listdir(path + "/"):

                # Если внутри архива есть ещё архивы
                if extracted_filename.endswith('.zip') or extracted_filename.endswith('.rar'):
                    
                    if platform.system() == "Windows":
                    # print("---II---> Extracting:", path + "/" + end_folder_to_extract + "/" + extracted_filename, "--->", path + "/" + end_folder_to_extract + extracted_filename[:-4])
                        extract_cmd(
                            path + "/" + extracted_filename,
                            path + "/" + extracted_filename[:-4]
                        )
                    elif platform.system() in ("Linux", "Darwin"):
                        patoolib.extract_archive(
                            path + "/" + extracted_filename,
                            outdir=path + "/" + extracted_filename[:-4])
                    
                    # patoolib.extract_archive(
                    #     path + "/" + end_folder_to_extract + extracted_filename,
                    #     outdir=path + "/" + end_folder_to_extract + extracted_filename[:-4], verbosity=-1)
                    # os.remove(path + "/" + end_folder_to_extract + extracted_filename)
                    rm_cmd(path + "/" + extracted_filename)
                    
                    for fname in os.listdir(path + "/" + extracted_filename[:-4]):
                        if os.path.isdir(path + "/" + extracted_filename[:-4] + "/" + fname):
                            all_filenames.append({
                                "type": "dir",
                                "path": path + "/" + extracted_filename[:-4] + "/" + fname
                            })
                        elif os.path.isfile(path + "/" + extracted_filename[:-4] + "/" + fname):
                            all_filenames.append({
                                "type": "file",
                                "path": path + "/" + extracted_filename[:-4] + "/" + fname
                            })
                    
                    # all_filenames += [
                    #     path + "/" + end_folder_to_extract + extracted_filename[:-4].replace("/", "_").replace("\\", "_") + "/" + fname
                    #     for fname in os.listdir(
                    #         path + "/" + end_folder_to_extract + extracted_filename[:-4].replace("/", "_").replace("\\", "_")
                    #     )
                    # ]
                    
                elif os.path.isdir(path + "/" + extracted_filename):
                    all_filenames.append({
                        "type": "dir",
                        "path": path + "/" + extracted_filename
                    })
                elif os.path.isfile(path + "/" + extracted_filename):
                    all_filenames.append({
                        "type": "file",
                        "path": path + "/" + extracted_filename.replace("/", "_").replace("\\", "_")
                    })
        else:
            path_from = "./"
            path_to = path + "/"
            copy_cmd(path_from, "./" + path_to, filename)
            #shutil.copy(filename, path + "/" + filename)
            all_filenames.append({
                "type": "file",
                "path": path_to + filename
            })

        if delete_after:
            os.remove(filename)


    print(f"FINAL FILES [R-{record.profile['bare']}]")
    for f in all_filenames:
        print(f"    > [{f['type']}] - {f['path']}")

    return path_on_share, all_filenames
