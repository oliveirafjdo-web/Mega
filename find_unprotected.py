import re

with open('app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if '@app.route' in line:
        route_line = line.strip()
        
        # Skip login/logout
        if '/login' in route_line or '/logout' in route_line:
            continue
            
        # Check next lines for @login_required
        has_login_required = False
        for j in range(i+1, min(i+3, len(lines))):
            if '@login_required' in lines[j]:
                has_login_required = True
                break
                
        if not has_login_required:
            print(f'Linha {i+1}: {route_line}')
            for k in range(i, min(i+5, len(lines))):
                print(f'  {k+1}: {lines[k].rstrip()}')
            print()
