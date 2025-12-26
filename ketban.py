import pandas as pd
import numpy as np
import heapq
import json
import os
import glob
from collections import deque
import re
import time

# ==========================================
# 1. TẢI DỮ LIỆU TỪ EXCEL
# ==========================================
def load_data(folder_path, json_filename='ketban.json'):
    excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))
    excel_files = [f for f in excel_files if not os.path.basename(f).startswith('~$')]
    
    if not excel_files:
        print("Lỗi: Không tìm thấy file Excel (.xlsx) trong E:\\ttnt")
        return None, {}, [], {}
    
    data_file = excel_files[0]
    print(f"--- Đang nạp dữ liệu: {os.path.basename(data_file)} ---")

    try:
        df = pd.read_excel(data_file, engine='openpyxl')
        json_path = os.path.join(folder_path, json_filename)
        with open(json_path, 'r', encoding='utf-8') as f:
            full_json = json.load(f)
            return (df, 
                    full_json.get('locations', {}), 
                    full_json.get('bonus_config', []), 
                    full_json.get('interest_groups', {}))
    except Exception as e:
        print(f"Lỗi hệ thống: {e}")
        return None, {}, [], {}

# ==========================================
# 2. ĐỐI TƯỢNG NGƯỜI DÙNG
# ==========================================
class User:
    def __init__(self, uid, name, dob, gender, location, interests, industry, marital, friends_str):
        def clean(val):
            if pd.isna(val) or str(val).strip() in ["", "nan", "-"]: return "-"
            return str(val).strip()

        self.id = str(uid)
        self.name = clean(name).title()
        self.dob = clean(dob)
        self.gender = clean(gender).title()
        self.location = clean(location).title()
        self.industry = clean(industry).title()
        self.marital = clean(marital).title()
        
        self.interests = [x.strip().title() for x in str(interests).split(';') if x.strip()] if clean(interests) != "-" else []
        self.friends_ids = [x.strip() for x in str(friends_str).split(',') if x.strip().isdigit()] if clean(friends_str) != "-" else []

    @classmethod
    def from_row(cls, row):
        return cls(row['Số thứ tự'], row['Họ và tên'], row['Ngày sinh'], row['Giới tính'], 
                   row['Nơi ở'], row['Sở thích'], row.get('Lĩnh vực/ngành nghề', '-'), 
                   row.get('Tình trạng hôn nhân', '-'), row.get('Bạn chung (ID)', ''))

class SocialGraph:
    def __init__(self, users, loc_map, bonus_rules, interest_groups):
        self.users = {u.id: u for u in users}
        self.adj_list = {u.id: set(u.friends_ids) for u in users}
        self.loc_map = loc_map
        self.bonus_rules = bonus_rules
        self.interest_groups = interest_groups
        
    def add_new_user(self, new_user):
        self.users[new_user.id] = new_user
        self.adj_list[new_user.id] = set()
        new_loc_val = self.loc_map.get(new_user.location)
        for uid, u in self.users.items():
            if uid == new_user.id: continue
            conn = False
            if new_user.location != "-" and new_user.location == u.location: conn = True
            elif new_loc_val is not None and self.loc_map.get(u.location) == new_loc_val: conn = True
            elif set(new_user.interests) & set(u.interests): conn = True
            if conn:
                self.adj_list[new_user.id].add(uid); self.adj_list[uid].add(new_user.id)

    def calculate_score(self, user_a, user_b):
        score = 0
        # 1. Trùng nơi ở (+1)
        if user_a.location != "-" and user_a.location == user_b.location: score += 1
        # 2. Ngành nghề (+1)
        if user_a.industry != "-" and user_a.industry == user_b.industry: score += 1
        # 3. Bạn chung (+1)
        if self.adj_list[user_a.id].intersection(self.adj_list[user_b.id]): score += 1
        # 4. Sở thích
        common = set(user_a.interests) & set(user_b.interests)
        score += len(common) * 2 # Trùng tuyệt đối
        if len(common) == 0: # Trùng trường (nhóm)
            a_grps = {g for g, ms in self.interest_groups.items() if any(m in user_a.interests for m in ms)}
            b_grps = {g for g, ms in self.interest_groups.items() if any(m in user_b.interests for m in ms)}
            if a_grps & b_grps: score += 1
        return score

# ==========================================
# 3. THUẬT TOÁN (BFS, DFS, A*)
# ==========================================
def run_bfs(graph, start_id):
    results = []
    queue = deque([start_id]); visited = {start_id}
    while queue:
        curr = queue.popleft()
        if curr != start_id:
            s = graph.calculate_score(graph.users[start_id], graph.users[curr])
            if s > 0: results.append({'user': graph.users[curr], 'score': s})
        for n in graph.adj_list.get(curr, []):
            if n in graph.users and n not in visited: visited.add(n); queue.append(n)
    return results

def run_dfs(graph, start_id, max_depth=3):
    results = []
    stack = [(start_id, 0)]; visited = {start_id}
    while stack:
        curr, depth = stack.pop()
        if curr != start_id:
            s = graph.calculate_score(graph.users[start_id], graph.users[curr])
            if s > 0: results.append({'user': graph.users[curr], 'score': s})
        if depth < max_depth:
            for n in graph.adj_list.get(curr, []):
                if n in graph.users and n not in visited: visited.add(n); stack.append((n, depth + 1))
    return results

def run_astar(graph, start_id, goal_id):
    open_set = [(0, start_id, [start_id])]; visited = set()
    while open_set:
        f, curr, path = heapq.heappop(open_set)
        if curr == goal_id: return path
        if curr in visited: continue
        visited.add(curr)
        for n in graph.adj_list.get(curr, []):
            if n in graph.users and n not in visited: heapq.heappush(open_set, (len(path), n, path + [n]))
    return None

# ==========================================
# 4. HIỂN THỊ
# ==========================================
def display_profile(u, label, me_id, graph, score, show_score=True):
    common_ids = graph.adj_list[me_id].intersection(graph.adj_list[u.id])
    common_names = [graph.users[cid].name for cid in common_ids if cid in graph.users]
    
    print(f"\n{label}. {u.name.upper()} (+{score})")
    print(f"Ngày sinh: {u.dob}")
    print(f"Giới tính: {u.gender}")
    print(f"Nơi ở: {u.location}")
    print(f"Ngành nghề: {u.industry}")
    print(f"Sở thích: {', '.join(u.interests) if u.interests else '-'} ")
    print(f"Tình trạng hôn nhân: {u.marital}")
    print(f"Bạn chung: {', '.join(common_names) if common_names else '-'} ")
    print("-" * 45)

def get_input():
    print("-" * 50); print("   NHẬP THÔNG TIN CỦA BẠN "); print("-" * 50)
    n = input("1. Họ và tên *: ").strip() or "-"
    d = input("2. Ngày sinh *: ").strip() or "-"
    g = input("3. Giới tính *: ").strip() or "-"
    l = input("4. Nơi ở *: ").strip() or "-"
    ind = input("5. Ngành nghề: ").strip() or "-"
    its = input("6. Sở thích (cách nhau bởi ;)*: ").strip() or "-"
    m = input("7. Tình trạng hôn nhân *: ").strip() or "-"
    return User("NEW_USER", n, d, g, l, its, ind, m, "")

def main():
    path = r"E:\ttnt"
    df, l_m, b_r, i_g = load_data(path)
    if df is None: return
    users = [User.from_row(r) for _, r in df.iterrows()]
    graph = SocialGraph(users, l_m, b_r, i_g)
    me = get_input()
    graph.add_new_user(me)

    start_exec = time.time()
    bfs_res = run_bfs(graph, me.id)
    dfs_res = run_dfs(graph, me.id)

    # 1. TOP 30 KẾT HỢP (ẨN ĐIỂM THEO YÊU CẦU CŨ - NHƯNG HIỆN ĐIỂM THEO YÊU CẦU MỚI)
    combined = {c['user'].id: c for c in bfs_res + dfs_res}.values()
    top_30_all = sorted(combined, key=lambda x: x['score'], reverse=True)[:30]
    print("\n" + "*"*60 + "\n DANH SÁCH TOP 30 NGƯỜI ĐÃ LỌC\n" + "*"*60)
    for i, c in enumerate(top_30_all): display_profile(c['user'], i+1, me.id, graph, c['score'], True)

    # TOP-1 CHI TIẾT
    if top_30_all:
        top_1 = top_30_all[0]
        print("\n" + "!"*60 + "\n          GỢI Ý PHÙ HỢP NHẤT (TOP-1)\n" + "!"*60)
        display_profile(top_1['user'], "TOP-1", me.id, graph, top_1['score'], True)

        # 2. ĐƯỜNG ĐI ĐẾN TOP 1
        print("\n CHI PHÍ/ĐƯỜNG ĐI ĐẾN TOP 1 (A*)")
        path = run_astar(graph, me.id, top_1['user'].id)
        if path: print(" -> ".join([graph.users[p].name for p in path]))
        else: print("Không tìm thấy đường đi.")

    print(f"\n THỜI GIAN THỰC THI : {time.time() - start_exec:.4f} giây")

    # 4. TOP 30 BFS
    print("\n" + "="*60 + "\n DANH SÁCH TOP 30 LỌC TỪ BFS \n" + "="*60)
    for i, c in enumerate(sorted(bfs_res, key=lambda x: x['score'], reverse=True)[:30]):
        display_profile(c['user'], i+1, me.id, graph, c['score'], True)

    # 5. TOP 30 DFS
    print("\n" + "="*60 + "\n DANH SÁCH TOP 30 LỌC TỪ DFS \n" + "="*60)
    for i, c in enumerate(sorted(dfs_res, key=lambda x: x['score'], reverse=True)[:30]):
        display_profile(c['user'], i+1, me.id, graph, c['score'], True)

    print(f"\n THỜI GIAN THỰC THI TỔNG CỘNG: {time.time() - start_exec:.4f} giây")

if __name__ == "__main__":
    main()