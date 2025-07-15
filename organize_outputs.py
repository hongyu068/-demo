#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动整理风控系统输出文件
将每次运行的结果整理到指定目录，便于查看和管理
"""

import os
import shutil
import datetime
import json
from pathlib import Path

def create_output_structure(base_dir):
    """创建输出目录结构"""
    directories = [
        'data_cleaned',           # 清洗后的数据
        'features',               # 特征工程结果
        'benford_results',        # Benford检测结果
        'clustering_results',     # 聚类分析结果
        'model_validation',       # 模型验证结果
        'dashboard',              # 仪表盘文件
        'reports',                # 报告文件
        'anomaly_users',          # 异常用户清单
        'logs',                   # 运行日志
        'summary'                 # 汇总信息
    ]
    
    for dir_name in directories:
        dir_path = os.path.join(base_dir, dir_name)
        os.makedirs(dir_path, exist_ok=True)
    
    return base_dir

def copy_file_with_timestamp(src, dst_dir, prefix=""):
    """复制文件并添加时间戳"""
    if not os.path.exists(src):
        return False
    
    # 获取文件名和扩展名
    filename = os.path.basename(src)
    name, ext = os.path.splitext(filename)
    
    # 添加时间戳
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{prefix}{name}_{timestamp}{ext}" if prefix else f"{name}_{timestamp}{ext}"
    
    dst_path = os.path.join(dst_dir, new_filename)
    shutil.copy2(src, dst_path)
    return dst_path

def organize_outputs():
    """整理所有输出文件"""
    # 创建输出目录
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"风控输出_{timestamp}"
    output_path = create_output_structure(output_dir)
    
    print(f"[INFO] 开始整理输出文件到: {output_dir}")
    
    # 定义文件映射关系
    file_mappings = {
        # 清洗后的数据
        'data_cleaned': [
            'data/cleaned_tx_log.csv',
            'data/risk_fact_table.csv',
            'data/user_risk_summary.csv'
        ],
        
        # 特征工程结果
        'features': [
            'data/features.csv',
            'data/amount_features.csv',
            'data/time_features.csv',
            'data/benford_expected.csv'
        ],
        
        # Benford检测结果
        'benford_results': [
            'data/benford_results.csv',
            'data/benford_risk_scores.csv',
            'data/benford_anomalies.csv'
        ],
        
        # 聚类分析结果
        'clustering_results': [
            'data/clustering_results.csv',
            'data/user_clusters.csv',
            'data/anomaly_clusters.csv'
        ],
        
        # 模型验证结果
        'model_validation': [
            'data/validation_results.csv',
            'data/cross_validation_scores.csv',
            'data/anomaly_recall_results.csv'
        ],
        
        # 仪表盘文件
        'dashboard': [
            'dashboard/interactive_dashboard.html',
            'dashboard/tableau_config.json',
            'dashboard/superset_config.json',
            'dashboard/README.md',
            'dashboard/summary_metrics.csv'
        ],
        
        # 报告文件
        'reports': [
            'reports/risk_monitoring_report.docx',
            'reports/risk_monitoring_report.md',
            'reports/risk_monitoring_report.json'
        ],
        
        # 异常用户清单
        'anomaly_users': [
            'data/anomaly_users.csv',
            'data/high_risk_users.csv',
            'data/suspicious_transactions.csv'
        ],
        
        # 运行日志
        'logs': [
            'logs/pipeline.log',
            'logs/error.log'
        ]
    }
    
    # 复制文件
    copied_files = {}
    for category, files in file_mappings.items():
        category_dir = os.path.join(output_path, category)
        copied_files[category] = []
        
        for file_path in files:
            if os.path.exists(file_path):
                try:
                    dst_path = copy_file_with_timestamp(file_path, category_dir)
                    if dst_path:
                        copied_files[category].append(os.path.basename(dst_path))
                        print(f"[OK] 已复制: {file_path} -> {dst_path}")
                except Exception as e:
                    print(f"[FAIL] 复制失败 {file_path}: {e}")
            else:
                print(f"[SKIP] 文件不存在: {file_path}")
    
    # 生成汇总信息
    generate_summary(output_path, copied_files, timestamp)
    
    print(f"\n[SUCCESS] 输出文件整理完成!")
    print(f"输出目录: {output_dir}")
    print(f"总文件数: {sum(len(files) for files in copied_files.values())}")
    
    return output_dir

def generate_summary(output_path, copied_files, timestamp):
    """生成汇总信息"""
    summary = {
        "运行时间": timestamp,
        "输出目录": output_path,
        "文件统计": {},
        "运行状态": "成功"
    }
    
    # 统计各类文件数量
    for category, files in copied_files.items():
        summary["文件统计"][category] = len(files)
    
    # 保存汇总信息
    summary_file = os.path.join(output_path, "summary", "run_summary.json")
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    
    # 生成可读的汇总报告
    summary_txt = os.path.join(output_path, "summary", "run_summary.txt")
    with open(summary_txt, 'w', encoding='utf-8') as f:
        f.write("=" * 50 + "\n")
        f.write("风控系统运行汇总报告\n")
        f.write("=" * 50 + "\n")
        f.write(f"运行时间: {timestamp}\n")
        f.write(f"输出目录: {output_path}\n")
        f.write(f"运行状态: 成功\n\n")
        
        f.write("文件统计:\n")
        f.write("-" * 20 + "\n")
        for category, files in copied_files.items():
            f.write(f"{category}: {len(files)} 个文件\n")
            for file in files:
                f.write(f"  - {file}\n")
            f.write("\n")
    
    print(f"[OK] 已生成汇总报告: {summary_file}")

def main():
    """主函数"""
    print("=== 风控系统输出文件整理工具 ===")
    
    try:
        output_dir = organize_outputs()
        print(f"\n[SUCCESS] 所有输出文件已整理到: {output_dir}")
        print("您可以打开该目录查看所有结果文件。")
        
    except Exception as e:
        print(f"[FAIL] 整理过程出错: {e}")

if __name__ == "__main__":
    main() 