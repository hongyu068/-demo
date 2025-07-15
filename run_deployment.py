#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化部署脚本
完成所有阶段的部署，不依赖Airflow
"""

import os
import sys
from datetime import datetime

def create_directories():
    """创建必要的目录"""
    directories = [
        'data',
        'dashboard', 
        'reports',
        'logs',
        'airflow'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"[OK] 创建目录: {directory}")

def create_simple_dag():
    """创建简化的DAG文件"""
    dag_content = """# 风险监测系统 DAG 配置
# 此文件用于Airflow调度，需要手动部署到Airflow DAG目录

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'risk_monitoring_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['admin@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'risk_monitoring_pipeline',
    default_args=default_args,
    description='高权限账户风险监测与异常识别流程',
    schedule_interval='0 2 * * *',  # 每天凌晨2点执行
    catchup=False,
    tags=['risk_monitoring', 'anomaly_detection']
)

# 任务定义
clean_data = BashOperator(
    task_id='clean_data',
    bash_command='cd /path/to/risk_monitoring && python pyspark/data_cleaning/clean_data.py',
    dag=dag
)

extract_features = BashOperator(
    task_id='extract_features',
    bash_command='cd /path/to/risk_monitoring && python pyspark/feature_engineering/extract_features.py',
    dag=dag
)

benford_analysis = BashOperator(
    task_id='benford_analysis',
    bash_command='cd /path/to/risk_monitoring && python pyspark/benford_detection/benford_analysis.py',
    dag=dag
)

behavior_clustering = BashOperator(
    task_id='behavior_clustering',
    bash_command='cd /path/to/risk_monitoring && python pyspark/clustering/behavior_clustering.py',
    dag=dag
)

model_validation = BashOperator(
    task_id='model_validation',
    bash_command='cd /path/to/risk_monitoring && python pyspark/validation/model_validation.py',
    dag=dag
)

create_dashboard = BashOperator(
    task_id='create_dashboard',
    bash_command='cd /path/to/risk_monitoring && python dashboard/create_dashboard.py',
    dag=dag
)

generate_report = BashOperator(
    task_id='generate_report',
    bash_command='cd /path/to/risk_monitoring && python reports/generate_report.py',
    dag=dag
)

# 任务依赖关系
clean_data >> extract_features >> benford_analysis
benford_analysis >> behavior_clustering >> model_validation
model_validation >> create_dashboard >> generate_report
"""
    
    with open('airflow/risk_monitoring_dag.py', 'w', encoding='utf-8') as f:
        f.write(dag_content)
    print("[OK] 创建Airflow DAG文件")

def create_deployment_script():
    """创建部署脚本"""
    deployment_script = """#!/bin/bash
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
"""
    
    with open('deploy.sh', 'w', encoding='utf-8') as f:
        f.write(deployment_script)
    
    # 设置执行权限（在Windows上可能不适用）
    try:
        os.chmod('deploy.sh', 0o755)
    except:
        pass
    
    print("[OK] 创建部署脚本")

def create_handover_documentation():
    """创建交接文档"""
    handover_doc = f"""# 风险监测系统交接文档

## 项目概述

本项目是一个高权限账户/交易日志风险监测与异常识别系统，通过Benford定律检测和行为聚类分析识别异常交易模式。

## 系统架构

### 技术栈
- **数据引擎**: Hive + Spark on YARN (PySpark 2.x/3.x)
- **开发IDE**: Cursor (支持SQL & Python混编)
- **Python依赖**: pandas, pyhive, pyspark, scikit-learn, scipy, seaborn/matplotlib, sqlalchemy
- **可视化**: Tableau Desktop / Apache Superset
- **调度**: Apache Airflow

### 目录结构
```
risk-monitoring/
├── sql/                    # SQL脚本目录
│   ├── data_ingestion/     # 数据接入脚本
│   ├── data_cleaning/      # 数据清洗脚本
│   └── views/              # 视图定义
├── pyspark/                # PySpark代码目录
│   ├── feature_engineering/ # 特征工程
│   ├── benford_detection/   # Benford定律检测
│   ├── clustering/         # 行为聚类
│   └── validation/         # 模型验证
├── dashboard/              # 可视化仪表盘
│   ├── tableau/           # Tableau文件
│   └── superset/          # Superset配置
├── airflow/               # Airflow DAG配置
├── config/                # 配置文件
├── data/                  # 示例数据
└── reports/               # 报告模板
```

## 部署说明

### 1. 环境准备
```bash
# 安装Python依赖
pip install -r requirements.txt

# 配置Hive连接
cp config/hive_config.example.py config/hive_config.py
# 编辑hive_config.py填入实际连接信息
```

### 2. 快速部署
```bash
# 运行部署脚本
chmod +x deploy.sh
./deploy.sh
```

### 3. 手动部署步骤
```bash
# 1. 测试连接
python sql/data_ingestion/test_connection.py

# 2. 数据清洗
python pyspark/data_cleaning/clean_data.py

# 3. 特征工程
python pyspark/feature_engineering/extract_features.py

# 4. Benford检测
python pyspark/benford_detection/benford_analysis.py

# 5. 行为聚类
python pyspark/clustering/behavior_clustering.py

# 6. 模型验证
python pyspark/validation/model_validation.py

# 7. 创建仪表盘
python dashboard/create_dashboard.py

# 8. 生成报告
python reports/generate_report.py
```

## 配置说明

### Hive配置 (config/hive_config.py)
```python
HIVE_CONFIG = {{
    'host': 'your-hive-server.com',
    'port': 10000,
    'username': 'your_username',
    'password': 'your_password',
    'database': 'default',
    'auth': 'CUSTOM',
    'configuration': {{
        'hive.server2.proxy.user': 'your_proxy_user'
    }}
}}
```

### 风险阈值配置
```python
RISK_THRESHOLDS = {{
    'benford_chi2_threshold': 15.0,
    'benford_mad_threshold': 0.015,
    'anomaly_score_threshold': 0.8,
    'night_operation_threshold': 0.3,
    'high_amount_threshold': 1000000
}}
```

## 监控和维护

### 1. 日常监控
- 检查Airflow DAG执行状态
- 监控数据质量指标
- 查看异常用户清单
- 验证模型性能指标

### 2. 定期维护
- 每周更新风险阈值
- 每月重新训练模型
- 每季度评估系统性能
- 每年更新技术栈

### 3. 故障处理
- 检查日志文件: logs/
- 验证数据源连接
- 重启失败的Airflow任务
- 联系技术支持

## 输出文件说明

### 数据文件 (data/)
- `risk_fact.csv`: 风险分析宽表
- `featured_data.csv`: 特征工程数据
- `benford_analysis.csv`: Benford分析结果
- `clustering_results.csv`: 聚类结果
- `anomaly_users.csv`: 异常用户清单

### 仪表盘文件 (dashboard/)
- `benford_heatmap_data.csv`: Benford热力图数据
- `cluster_scatter_data.csv`: 聚类散点图数据
- `anomaly_watchlist.csv`: 异常监控列表
- `interactive_dashboard.html`: 交互式仪表盘
- `tableau_config.json`: Tableau配置
- `superset_config.json`: Superset配置

### 报告文件 (reports/)
- `risk_monitoring_report.docx`: Word格式报告
- `risk_monitoring_report.md`: Markdown格式报告
- `risk_monitoring_report.json`: JSON格式报告

## 联系信息

- **项目负责人**: [姓名]
- **技术支持**: [邮箱]
- **运维团队**: [联系方式]

## 更新记录

| 日期 | 版本 | 更新内容 | 负责人 |
|------|------|----------|--------|
| {datetime.now().strftime('%Y-%m-%d')} | 1.0.0 | 初始版本 | [姓名] |

---
*本文档由风险监测系统自动生成，最后更新时间: {datetime.now().strftime('%Y年%m月%d日')}*
"""
    
    with open('HANDOVER_DOCUMENTATION.md', 'w', encoding='utf-8') as f:
        f.write(handover_doc)
    print("[OK] 创建交接文档")

def create_quick_start_guide():
    """创建快速开始指南"""
    quick_start = """# 风险监测系统快速开始指南

## 5分钟快速上手

### 1. 环境检查
```bash
# 检查Python版本 (需要3.8+)
python --version

# 检查依赖包
pip list | grep -E "(pandas|pyspark|scikit-learn)"
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 配置连接
```bash
# 复制配置文件
cp config/hive_config.example.py config/hive_config.py

# 编辑配置文件 (填入实际连接信息)
# 如果无法连接Hive，系统会自动使用示例数据
```

### 4. 运行测试
```bash
# 测试连接
python sql/data_ingestion/test_connection.py

# 如果连接成功，会显示数据概览
# 如果连接失败，会创建示例数据
```

### 5. 完整流程测试
```bash
# 一键运行完整流程
python pyspark/data_cleaning/clean_data.py
python pyspark/feature_engineering/extract_features.py
python pyspark/benford_detection/benford_analysis.py
python pyspark/clustering/behavior_clustering.py
python pyspark/validation/model_validation.py
python dashboard/create_dashboard.py
python reports/generate_report.py
```

### 6. 查看结果
- 打开 `dashboard/interactive_dashboard.html` 查看交互式仪表盘
- 查看 `reports/risk_monitoring_report.md` 阅读报告
- 检查 `data/anomaly_users.csv` 查看异常用户

## 常见问题

### Q: 连接Hive失败怎么办？
A: 系统会自动创建示例数据进行测试，不影响功能验证。

### Q: 如何修改风险阈值？
A: 编辑 `config/hive_config.py` 中的 `RISK_THRESHOLDS` 配置。

### Q: 如何添加新的检测规则？
A: 在 `pyspark/clustering/behavior_clustering.py` 的 `identify_anomalies` 函数中添加新规则。

### Q: 如何自定义报告格式？
A: 修改 `reports/generate_report.py` 中的报告生成函数。

## 下一步

1. 阅读完整文档: `HANDOVER_DOCUMENTATION.md`
2. 配置生产环境连接
3. 设置Airflow调度
4. 集成到现有监控系统

---
*更多详细信息请参考完整文档*
"""
    
    with open('QUICK_START.md', 'w', encoding='utf-8') as f:
        f.write(quick_start)
    print("[OK] 创建快速开始指南")

def main():
    """主函数"""
    print("=== 风险监测系统部署 ===")
    
    # 创建目录
    create_directories()
    
    # 创建文件
    create_simple_dag()
    create_deployment_script()
    create_handover_documentation()
    create_quick_start_guide()
    
    print("\n=== 部署完成 ===")
    print("生成的文件:")
    print("- airflow/risk_monitoring_dag.py")
    print("- deploy.sh")
    print("- HANDOVER_DOCUMENTATION.md")
    print("- QUICK_START.md")
    
    print("\n=== 项目完成 ===")
    print("所有阶段已完成，系统已准备就绪！")
    print("请按照 QUICK_START.md 进行快速测试")

if __name__ == "__main__":
    main() 