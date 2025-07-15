#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量修复Python文件中的Unicode特殊符号
将 [OK]、[FAIL]、[WARN]、[SUCCESS]、[WARN]️ 等符号替换为普通字符
"""

import os
import re

def fix_unicode_in_file(file_path):
    """修复单个文件中的Unicode符号"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 替换Unicode符号
        replacements = {
            '[OK]': '[OK]',
            '[FAIL]': '[FAIL]',
            '[WARN]': '[WARN]',
            '[SUCCESS]': '[SUCCESS]',
            '[WARN]️': '[WARN]'
        }
        
        original_content = content
        for old, new in replacements.items():
            content = content.replace(old, new)
        
        # 如果内容有变化，写回文件
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[OK] 已修复: {file_path}")
            return True
        else:
            print(f"[SKIP] 无需修复: {file_path}")
            return False
            
    except Exception as e:
        print(f"[FAIL] 修复失败 {file_path}: {e}")
        return False

def find_python_files(directory):
    """查找目录下的所有Python文件"""
    python_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    return python_files

def main():
    """主函数"""
    print("=== 批量修复Unicode符号 ===")
    
    # 查找所有Python文件
    python_files = find_python_files('.')
    print(f"找到 {len(python_files)} 个Python文件")
    
    # 修复每个文件
    fixed_count = 0
    for file_path in python_files:
        if fix_unicode_in_file(file_path):
            fixed_count += 1
    
    print(f"\n=== 修复完成 ===")
    print(f"总文件数: {len(python_files)}")
    print(f"修复文件数: {fixed_count}")
    print(f"跳过文件数: {len(python_files) - fixed_count}")

if __name__ == "__main__":
    main() 