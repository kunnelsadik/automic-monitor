import os

def validate_files(rules_df):
    failures = []
    for _, rule in rules_df.iterrows():
        path = rule["path"]
        min_files = rule["min_files"]

        count = len(os.listdir(path)) if os.path.exists(path) else 0
        if count < min_files:
            failures.append({
                "path": path,
                "expected": min_files,
                "actual": count
            })
    return failures
