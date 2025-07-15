#!/bin/bash
# 风险监测系统部署脚本

echo "=== 风险监测系统部署开始 ==="

# 1. 检查环境
echo "检查Python环境..."
python --version
pip --version

# 2. 安装依赖
echo "安装Python依赖..."
pip install -r requirements.txt

# 3. 创建必要目录
echo "创建目录结构..."
mkdir -p data dashboard reports logs

# 4. 配置Hive连接
echo "配置Hive连接..."
if [ ! -f "config/hive_config.py" ]; then
    cp config/hive_config.example.py config/hive_config.py
    echo "请编辑 config/hive_config.py 文件，填入实际的Hive连接信息"
fi

# 5. 测试连接
echo "测试Hive连接..."
python sql/data_ingestion/test_connection.py

# 6. 运行完整流程测试
echo "运行完整流程测试..."
python pyspark/data_cleaning/clean_data.py
python pyspark/feature_engineering/extract_features.py
python pyspark/benford_detection/benford_analysis.py
python pyspark/clustering/behavior_clustering.py
python pyspark/validation/model_validation.py
python dashboard/create_dashboard.py
python reports/generate_report.py

# 7. 设置权限
echo "设置文件权限..."
chmod +x *.py
chmod +x pyspark/*/*.py
chmod +x sql/*/*.py
chmod +x dashboard/*.py
chmod +x reports/*.py

echo "=== 部署完成 ==="
echo "请检查以下文件："
echo "- data/ 目录下的数据文件"
echo "- dashboard/ 目录下的仪表盘文件"
echo "- reports/ 目录下的报告文件"
echo "- logs/ 目录下的日志文件"
