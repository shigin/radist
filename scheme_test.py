import radist

files = []
for i in '000', '001', '002':
    files.append(radist.get_file('ra://ix.merger.%s//etc/passwd' % i))

users = {
         'root': 0,
         'shigin': 0,
         'search': 0,
        } 

for line in radist.MuxReader(files):
    user = line.split(':')[0]
    if user in users:
        users[user] += 1
    
for user, count in users.items():
    assert count == 3, "detect user %s on %d servers" % (user, count)
