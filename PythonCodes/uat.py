arr = [12, 45, 7, 89, 34, 2]

smallest = largest = arr[0]

for num in arr:
    if num < smallest:
        smallest = num
    elif num > largest:
        largest = num

print("Smallest:", smallest)
print("Largest:", largest)
