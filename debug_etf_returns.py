#!/usr/bin/env python3
"""
验证脚本：测试新的 NAV 计算逻辑
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.providers.akshare_api import AkShareProvider
from src.services.assemblers import BaseAssembler
from datetime import datetime

def verify_new_logic(code="513300"):
    print(f"\n=== 验证新的 NAV 计算逻辑 ({code}) ===")
    
    # 1. 获取 NAV 历史
    print("Fetching NAV history...")
    history = AkShareProvider.get_nav_history_em(code)
    
    if not history:
        print("[ERROR] No history found")
        return
        
    print(f"History length: {len(history)}")
    print(f"Latest: {history[-1]}")
    
    # 2. 计算收益率
    print("\nCalculating returns...")
    ret_1y = BaseAssembler.calculate_period_return(history, 365)
    ret_6m = BaseAssembler.calculate_period_return(history, 183)
    ret_1m = BaseAssembler.calculate_period_return(history, 30)
    
    print(f"1 Year Return (365 days): {ret_1y*100:.2f}%" if ret_1y is not None else "1 Year: N/A")
    print(f"6 Month Return (183 days): {ret_6m*100:.2f}%" if ret_6m is not None else "6 Month: N/A")
    print(f"1 Month Return (30 days): {ret_1m*100:.2f}%" if ret_1m is not None else "1 Month: N/A")
    
    # 3. 对比说明
    print("\n=== 结果说明 ===")
    print("券商当前显示: 1Y=7.98%, 6M=11.14% (基于 T-1 数据)")
    print(f"我们当前计算: 1Y={ret_1y*100:.2f}%, 6M={ret_6m*100:.2f}% (基于 T 数据)")
    
    if ret_1y and abs(ret_1y*100 - 13.62) < 0.5:
        print("✅ 1Y 结果符合预期 (约 13.62%) - 比券商更有时效性")
    else:
        print("⚠️ 1Y 结果与预期可能有偏差，请手动检查")

    if ret_6m and abs(ret_6m*100 - 12.62) < 2.0:
        print("✅ 6M 结果符合预期 (约 12.6%)")
    else:
        print("⚠️ 6M 结果与预期可能有偏差")

if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "513300"
    verify_new_logic(code)
