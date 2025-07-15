#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
环境设置脚本
统一配置Spark环境变量，解决Driver/Worker版本不一致问题
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

def setup_spark_environment():
    """设置Spark环境变量"""
    print("=== 设置Spark环境变量 ===")
    
    # 1. 统一Python版本
    python_exe = sys.executable
    print(f"当前Python路径: {python_exe}")
    print(f"当前Python版本: {sys.version}")
    
    # 设置Spark使用的Python版本
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
    
    # 2. Windows特殊处理
    if platform.system() == "Windows":
        # 设置HADOOP_HOME（如果不存在）
        hadoop_home = os.environ.get("HADOOP_HOME")
        if not hadoop_home:
            # 尝试几个常见路径
            possible_paths = [
                "C:/hadoop",
                "C:/winutils",
                Path.home() / "hadoop"
            ]
            
            for path in possible_paths:
                if Path(path).exists():
                    hadoop_home = str(path)
                    break
            
            if hadoop_home:
                os.environ["HADOOP_HOME"] = hadoop_home
                print(f"设置HADOOP_HOME: {hadoop_home}")
            else:
                print("[WARN] 未找到HADOOP_HOME，可能会有警告信息")
        
        # 设置PATH
        if hadoop_home:
            bin_path = Path(hadoop_home) / "bin"
            if bin_path.exists():
                current_path = os.environ.get("PATH", "")
                os.environ["PATH"] = f"{bin_path};{current_path}"
    
    # 3. Spark配置
    spark_configs = {
        "SPARK_LOCAL_IP": "127.0.0.1",
        "SPARK_DRIVER_HOST": "127.0.0.1",
        "SPARK_DRIVER_BIND_ADDRESS": "127.0.0.1"
    }
    
    for key, value in spark_configs.items():
        os.environ[key] = value
        print(f"设置{key}: {value}")
    
    print("[OK] Spark环境变量设置完成")

def check_dependencies():
    """检查依赖包"""
    print("\n=== 检查依赖包 ===")
    
    required_packages = [
        "pandas", "numpy", "scipy", "plotly", "dash", 
        "sklearn", "pyspark"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} - 缺失")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n[WARN] 缺少以下包: {', '.join(missing_packages)}")
        print("请运行: pip install -r requirements.txt")
        return False
    
    print("[OK] 所有依赖包检查通过")
    return True

def test_spark_session():
    """测试Spark会话创建"""
    print("\n=== 测试Spark会话 ===")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        
        spark = SparkSession.builder \
            .appName("EnvironmentTest") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.ui.port", "4040") \
            .config("spark.port.maxRetries", "100") \
            .config("spark.pyspark.python", os.environ["PYSPARK_PYTHON"]) \
            .config("spark.pyspark.driver.python", os.environ["PYSPARK_DRIVER_PYTHON"]) \
            .getOrCreate()
        
        # 测试基本操作
        test_data = [("test", 1), ("data", 2)]
        df = spark.createDataFrame(test_data, ["col1", "col2"])
        
        # 测试cast函数（替代try_cast）
        df_cast = df.withColumn("col2_double", F.cast("col2", "double"))
        
        result = df_cast.collect()
        print(f"✓ Spark会话创建成功，测试数据: {result}")
        
        spark.stop()
        print("[OK] Spark测试通过")
        return True
        
    except Exception as e:
        print(f"[FAIL] Spark测试失败: {e}")
        return False

def create_directories():
    """创建必要的目录"""
    print("\n=== 创建目录结构 ===")
    
    directories = [
        "data", "logs", "reports", "dashboard", 
        "outputs", "temp", "checkpoints"
    ]
    
    for dir_name in directories:
        Path(dir_name).mkdir(exist_ok=True)
        print(f"✓ {dir_name}/")
    
    print("[OK] 目录结构创建完成")

def main():
    """主函数"""
    print("=== 风控系统环境设置 ===")
    
    # 1. 创建目录
    create_directories()
    
    # 2. 设置环境变量
    setup_spark_environment()
    
    # 3. 检查依赖
    deps_ok = check_dependencies()
    
    # 4. 测试Spark
    if deps_ok:
        spark_ok = test_spark_session()
    else:
        spark_ok = False
    
    # 5. 总结
    print("\n=== 环境设置总结 ===")
    print(f"依赖检查: {'✓ 通过' if deps_ok else '✗ 失败'}")
    print(f"Spark测试: {'✓ 通过' if spark_ok else '✗ 失败'}")
    
    if deps_ok and spark_ok:
        print("\n[SUCCESS] 环境设置完成！可以运行风控系统")
        print("下一步: python run_full_pipeline_simple.py")
    else:
        print("\n[FAIL] 环境设置有问题，请检查上述错误")
        if not deps_ok:
            print("建议: pip install -r requirements.txt")

if __name__ == "__main__":
    main() 