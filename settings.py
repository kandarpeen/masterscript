import yaml

with open("testing.settings", "r") as f:
    iomap = yaml.safe_load(f)

print(iomap.get("classes"))