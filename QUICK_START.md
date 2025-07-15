# 风险监测系统快速开始指南

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
