

# Reverse string without using built-in functions:

def reverse_string(s):
    result = ""
    for char in s:
        result = char + result
    return result

print(reverse_string("Cognizant"))  # Output: tnizgnoC

#OR-

def reverse_string(s):
    rstring = ""
    for i in s:
        rstring =i+rstring
    return rstring
print(reverse_string(input("Enter a string: ")))


# Write a function without using builtin fucntion for SWAP CASE- swapcase()


def swap_case(s):
    result = ""
    for c in s:
        if c.islower():        # if character is lowercase
            result =result+c.upper() 
        elif c.isupper():      # if character is uppercase
            result =result+c.lower()
        else:                  # keep digits, spaces, symbols same
            result =result+c
    return result

print(swap_case(input("Enter a string: ")))




# Count Vowel and Consonant  in a given word:

vowel = ["a", 'e', 'i', 'o', 'u']

word = input("Enter a word: ")
count = 0
cct=0

for letter in word:
    if letter not in vowel:
        cct += 1
    else:
        count += 1
        print("Vowel", count)


print("consonant count",cct)

#function:


def vowel_consonant(string):
    vowels = ['a', 'e', 'i', 'o', 'u']
    vcount = 0
    ccount = 0
    for char in string.lower():
        if char in vowels:
            vcount += 1
        else:
            ccount += 1

    return vcount, ccount

print(vowel_consonant(input("Enter A String:")))


# Counting the number of occurrences of a character in a String:

def char_count(string):
    count = 0
    char="a"
    for i in string:
        if char == i:
            count =count+1
    return count
print(char_count(input("Enter a string: ")))


#Writing A FIBONACCI SERIES:

fib=[0,1]
n=10

for i in range(n):
    fib.append(fib[-1]+fib[-2])
print(fib)

print(', '.join(str(e) for e in fib))


#function:

def fibonacci_series(n):
    a, b = 0, 1
    series = []
    for _ in range(n):
        series.append(a)
        a, b = b, a + b
    return series


# Example usage
n = int(input("Enter number of terms: "))
print("Fibonacci Series:", fibonacci_series(n))

#—easy————
a,b=1,2
series=[]
n=int(input("Please input the number of elements: "))

for i in range(n):
    series.append(a)
    a,b=b,a+b
print(series)

#————————SPLIT and JOIN————

def split_and_join(line):
    a = line.split(" ")
    b = "-".join(a)
    return b


if __name__ == '__main__':
    line = input("Enter a line: ")
    result = split_and_join(line)
    print(result)


#———————FIRST NAME, LAST NAME—————

def print_full_name(first, last):
    # Write your code here

   print(f"Hello {first} {last}! You just delved into python.")



if __name__ == '__main__':
    first_name = input("Enter your first name: ")
    last_name = input("Enter your last name: ")

print_full_name(first_name, last_name)

#—————GIVING SPACE ON BASIS ON CAP LETTER—————

def Separate_Words(text):
    newstring = ""
    for char in text:
        if char.isupper():
            newstring = newstring + " " + char
        else:
            newstring = newstring + char
    return newstring

# Example usage
print(Separate_Words(input("Enter a sentence in Small and Cap Letter: Without Space")))


#——————— INSERT INTO STRING————

def mutate_string(string, position, character):
    l = list(string)
    l[position] = character
    string = ''.join(l)
    return string


if __name__ == '__main__':
    s = input("Enter a string: ")
    i, c = input("Enter Pos and Char").split()
    s_new = mutate_string(s, int(i), c)
    print(s_new)




