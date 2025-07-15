#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一键运行脚本
按顺序执行所有阶段的脚本，完成完整的风险监测流程
"""

import os
import sys
import subprocess
from datetime import datetime
import locale

def run_command(command, description):
    """运行命令并显示结果"""
    print(f"\n{'='*50}")
    print(f"执行: {description}")
    print(f"命令: {command}")
    print(f"{'='*50}")
    
    try:
        encoding = locale.getpreferredencoding()
        result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding=encoding)
        print("输出:")
        print(result.stdout)
        if result.stderr:
            print("错误:")
            print(result.stderr)
        
        if result.returncode == 0:
            print(f"[OK] {description} 执行成功")
            return True
        else:
            print(f"[FAIL] {description} 执行失败")
            return False
            
    except Exception as e:
        print(f"[FAIL] {description} 执行异常: {e}")
        return False

def main():
    """主函数"""
    print("=== 风险监测系统完整流程执行 ===")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 定义执行步骤
    steps = [
        {
            'command': 'python sql/data_ingestion/test_connection.py',
            'description': '阶段2.1: 数据接入测试'
        },
        {
            'command': 'python pyspark/data_cleaning/clean_data.py',
            'description': '阶段2.2: 数据清洗'
        },
        {
            'command': 'python pyspark/feature_engineering/extract_features.py',
            'description': '阶段2.3: 特征工程'
        },
        {
            'command': 'python pyspark/benford_detection/benford_analysis.py',
            'description': '阶段2.3: Benford检测'
        },
        {
            'command': 'python pyspark/clustering/behavior_clustering.py',
            'description': '阶段2.4: 行为聚类'
        },
        {
            'command': 'python pyspark/validation/model_validation.py',
            'description': '阶段2.5: 模型验证'
        },
        {
            'command': 'python dashboard/create_dashboard.py',
            'description': '阶段2.6: 创建仪表盘'
        },
        {
            'command': 'python reports/generate_report.py',
            'description': '阶段2.6: 生成报告'
        }
    ]
    
    # 执行每个步骤
    success_count = 0
    total_steps = len(steps)
    
    for i, step in enumerate(steps, 1):
        print(f"\n进度: {i}/{total_steps}")
        
        success = run_command(step['command'], step['description'])
        if success:
            success_count += 1
        
        # 如果某个步骤失败，询问是否继续
        if not success:
            print(f"\n步骤 '{step['description']}' 执行失败")
            response = input("是否继续执行后续步骤? (y/n): ")
            if response.lower() != 'y':
                print("用户选择停止执行")
                break
    
    # 显示执行结果
    print(f"\n{'='*60}")
    print("执行结果汇总")
    print(f"{'='*60}")
    print(f"总步骤数: {total_steps}")
    print(f"成功步骤数: {success_count}")
    print(f"失败步骤数: {total_steps - success_count}")
    print(f"成功率: {success_count/total_steps*100:.1f}%")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if success_count == total_steps:
        print("\n[SUCCESS] 所有步骤执行成功！")
        print("请查看以下文件:")
        print("- dashboard/interactive_dashboard.html (交互式仪表盘)")
        print("- reports/risk_monitoring_report.md (风险监测报告)")
        print("- data/anomaly_users.csv (异常用户清单)")
        
        # 自动整理输出文件
        print(f"\n{'='*60}")
        print("正在自动整理输出文件...")
        print(f"{'='*60}")
        
        try:
            # 调用输出整理脚本
            organize_result = run_command('python organize_outputs.py', '自动整理输出文件')
            if organize_result:
                print("\n[SUCCESS] 输出文件整理完成！")
                print("所有结果文件已自动整理到指定目录，便于查看和管理。")
            else:
                print("\n[WARN] 输出文件整理失败，但主要流程已完成。")
        except Exception as e:
            print(f"\n[WARN] 输出文件整理异常: {e}")
            print("主要流程已完成，您可以手动运行 'python organize_outputs.py' 来整理文件。")
    else:
        print(f"\n[WARN] 有 {total_steps - success_count} 个步骤执行失败")
        print("请检查错误信息并重新运行失败的步骤")
    
    print(f"\n{'='*60}")

if __name__ == "__main__":
    main() 