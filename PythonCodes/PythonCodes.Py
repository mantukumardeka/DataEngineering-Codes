#===============================================
#- Example of DeepCopy() and Shallow Copy()
#===============================================
print("Here all about DEEP COPY and SHALLOW COPY")

import copy

# -----------------------------
# 🧠 DEEP COPY EXAMPLE
# -----------------------------
print("\nDEEP COPY EXAMPLE")

a = [1, 2, 3, [4, 5], 6]
print("Original list a:", a)

# Deep copy
a1 = copy.deepcopy(a)
print("Deep copied list a1:", a1)

# Modify original
a[3].append(7)
a.append(8)

print("\nAfter modifying 'a':")
print("a  =", a)
print("a1 =", a1)

print("\n👉 In Deep Copy, changes in nested lists of 'a' do NOT affect 'a1'.")

# -----------------------------
# 💡 SHALLOW COPY EXAMPLE
# -----------------------------
print("\nSHALLOW COPY EXAMPLE")

x = [1, 2, 3, [4, 5], 6]
print("Original list x:", x)

# Shallow copy
x1 = copy.copy(x)
print("Shallow copied list x1:", x1)

# Modify original
x[3].append(7)
x.append(9)

print("\nAfter modifying 'x':")
print("x  =", x)
print("x1 =", x1)

print("\n👉 In Shallow Copy, changes inside nested lists reflect in 'x1',")
print("but changes to the top-level list (like append) do not affect 'x1'.")

#=================================
#-- Write a function for Reverse a String:
#=================================
def reverse_str(str):
    nst=""

    for i in str:
        if i in str:
            nst= i + nst
    print(nst)     
reverse_str(input("Enter a String")) 

#===========================================
# Write a Function for SWAP CASES:
#============================================


def swap_string(st):

    nst=""

    for i in st:
        if i.islower():
            nst=nst +i.upper()
        elif i.isupper():
            nst=nst+i.lower()
    print(nst)

    

swap_string(input("Enter a String"))

#============================================
#Count Vowel and Consonant  in a given word:
#============================================

def vowel_consonent_count(str):
    vowel=["a","e","i","o","u"]

    vc=0
    cc=0

    for i in str.lower():
        if i.isalpha(): # Count only alphabetic characters (ignore spaces, digits, punctuation)
            if i in vowel:
                vc=vc+1
            else:
                cc=cc+1
    print(f"Vowel Count: {vc} /n Consonent Count: {cc}") 
vowel_consonent_count(input("Enter A Sting : "))    

#==========================================================
#READ A FILE FROM DESKTOP AND COUNT VOWEL AND CONSONENT:
#==========================================================




file_path="/Users/mantukumardeka/Desktop/data/repeated_words.txt"

with open (file_path,"r") as file:

 data=file.read().strip()

print(data)

vowel="aeiou"

vc=0
cc=0
for i in data:
 if i.isalpha():
    if i in vowel:
        vc=vc+1
    else:
        cc=cc+1
print(f"Vowe:{vc},\n Consonent:{cc}")  

#==========Using Function==============

def vc_count(file_path):
    # Open and read file
    with open(file_path, "r", encoding="utf-8") as file:
        data = file.read().strip()

    # Initialize counters
    vowels = ["a", "e", "i", "o", "u"]
    vc = 0
    cc = 0

    # Count vowels and consonants
    for ch in data.lower():  # convert to lowercase for uniformity
        if ch.isalpha():  # only letters
            if ch in vowels:
                vc += 1
            else:
                cc += 1

    print(f"Vowels: {vc}\nConsonants: {cc}")


# 🔹 Run the function
vc_count(input("Enter file path: "))
# =====================