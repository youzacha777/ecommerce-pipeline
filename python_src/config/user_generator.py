#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
User Generator
- Faker를 사용하여 가상의 유저 데이터를 생성
- users.json 파일로 저장
"""

import json
import os
from faker import Faker

# Faker 초기화
fake = Faker()

# 유저 수 설정
NUM_USERS = 200

# 실행 스크립트 위치 기준 경로
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "users.json")

def generate_users(num_users):
    users = []
    for _ in range(num_users):
        user = {
            "user_id": fake.unique.bothify(text="U###??"),  # 예: U123AB
            "name": fake.name(),
            "email": fake.email()
        }
        users.append(user)
    return users

def main():
    users = generate_users(NUM_USERS)
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(users, f, indent=4, ensure_ascii=False)
        print(f"[INFO] users.json 생성 완료: {OUTPUT_FILE}")
    except Exception as e:
        print(f"[ERROR] users.json 생성 실패: {e}")

if __name__ == "__main__":
    main()
