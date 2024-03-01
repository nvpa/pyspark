num_dict = {
    'key1': [1, 2, 3, 1],
    'key2': [5, 1, 7, 1],
    'key3': [1, 10, 1, 12]
}

# Initialize the output dictionary with keys from the first key and values as positive infinity
output_dict = dict.fromkeys(num_dict, float('inf'))

# Iterate through the keys in num_dict
for key in num_dict:
    # Update the values in the output_dict by taking the minimum value at each position
    output_dict = {k: min(output_dict[k], num_dict[key][i]) for i, k in enumerate(output_dict)}

# Convert the output_dict values to a list
output = list(output_dict.values())

print(output)

x = 'nahfjadjd'
print(x.split(','))
print(list(x))


def fib(n):
    p, q = 0, 1
    while (p < n):
        yield p
        p, q = q, p + q


x = fib(10)
for i in x:
    print(i)


def tellArguments(**kwargs):
    for key, value in kwargs.items():
        print(key + ": " + value)


tellArguments(arg1="argument 1", arg2="argument 2", arg3="argument 3")


class parent:
    def __init__(self, name):
        self.name = name

    def pat_name(self):
        return self.name


class child(parent):
    def chi_name(self):
        return self.name


obj = child("naga")
print(obj.chi_name())

import pandas as pd

dict_info = {'key1': 2.0, 'key2': 3.1, 'key3': 2.2}
series_obj = pd.Series(dict_info)
print(series_obj)


def factorial(x):
    if x == 1:
        return 1
    else:
        return x * factorial(x - 1)


f = factorial(5)
print(f)
l = [1, 1, 2, 3, 34, 5]
l1 = []
for i in l:
    l1.insert(0, i)
    print(l1)

for i in range(len(l) - 1, -1, -1):
    print(l[i])
N = "nagarjuna"

for i in range(0, len(N)):
    for j in range(0, len(N)):
        if i == j:
            print(N[i], end='')
        else:
            print(end=" ")
    print()


def digit(n):
    sum = 0
    while (n > 0):
        reminder = n % 10
        sum = sum + reminder
        n = n // 10
    return sum


print(digit(675))
n = 876
reverse = 0
while (n > 0):
    reminder = n % 10  # 876%10=6
    reverse = (reverse * 10) + reminder
    n = n // 10  # 876//10()
print('the reverse of number is', reverse)

for i in range(1, 5):
    for j in range(1, i + 1):
        print(j, end=' ')
    print()

my_list = [[1, 2, 3], [4, 5, 6], [6, 7, 8]]
x=[j for i in my_list for j in i ]
print(x)

# Use a loop to convert each sublist to a string
output = []
for sublist in my_list:
    output.append(','.join(map(str, sublist)))

# Print the result
for item in output:
    print(item)
