# DMP

to add your Census API Key, do the following in your terminal (wherever you are running the code)
1️⃣ Open your .zshrc 
```bash
nano ~/.zshrc
```

Make sure you see exactly this line, somewhere (preferably near the bottom):
```bash
export CENSUS_API_KEY="778860d048af2c5ac7152288a37a38c790c4aadd"
```

Check carefully: no extra spaces, no smart quotes, no line breaks

Save and exit:
Ctrl + O → Enter
Ctrl + X

2️⃣ Reload it explicitly

Run:
```bash
source ~/.zshrc
```

3️⃣ Verify at the shell level
```bash
echo $CENSUS_API_KEY
```
