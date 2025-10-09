class Employee:
    def __init__(self, name, salary):
        self.name = name
        self.salary = salary

    def give_raise(self, amount):
        self.salary += amount
        print(f"{self.name}'s new salary: {self.salary}")

# Objects
emp1 = Employee("Alice", 50000)
emp2 = Employee("Bob", 60000)

emp1.give_raise(5000)  # Alice's new salary: 55000
emp2.give_raise(10000) # Bob's new salary: 70000


## OBJECT: class ko call karke jaha rakhte hai vo object hai
