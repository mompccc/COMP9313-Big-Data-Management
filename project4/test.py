with open('tiny-data.txt') as file:
    data = [line.strip().split(' ')[1:] for line in file]

p = 0.5
for i in range(len(data)):
    for j in range(i+1, len(data)):
        total = data[i] + data[j]
        total_set = set(total)
        x = []
        for word in total_set:
            if word in data[i] and word in data[j]:
                x.append(word)
        rate = len(x)/len(total_set)
        temp = (i, j)
        if rate >= p:
            print(temp, rate)
