# DMP

to add your Census API Key, do the following in your terminal (wherever you are running the code)
1️⃣ Open your .zshrc 
'''
nano ~/.zshrc
'''

Make sure you see exactly this line, somewhere (preferably near the bottom):
'''
export CENSUS_API_KEY="778860d048af2c5ac7152288a37a38c790c4aadd"
'''

Check carefully: no extra spaces, no smart quotes, no line breaks

Save and exit:
Ctrl + O → Enter
Ctrl + X

2️⃣ Reload it explicitly

Run:
'''
source ~/.zshrc
'''

3️⃣ Verify at the shell level
'''
echo $CENSUS_API_KEY
'''


You must see the key printed.

If you don’t, stop here and tell me — that means .zshrc isn’t being read.
