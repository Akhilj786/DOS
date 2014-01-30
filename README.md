
Distributed Operating System Project-1


Lucas Square problem:
An interesting problem in arithmetic with deep implications to elliptic curve
theory is the problem of ending perfect squares that are sums of consecutive
squares. A classic example is the Pythagorean identity:
32 + 42 = 52 (1)
that reveals that the sum of squares of 3; 4 is itself a square. A more interesting
example is Lucas` Square Pyramid :
12 + 22 + ::: + 242 = 702 (2)
In both of these examples, sums of squares of consecutive integers form the
square of another integer.
The goal of this rst project is to use Scala and the actor model to build a
good solution to this problem that runs well on multi-core machines.


Our Approach:
For this project of determining the perfect squares that are 
squares of consequetive numbers and printing the first number 
of the series, we have zeroed in on using 1 master Actor and 8 
child Actors. For numbers that are less than 8, we use a single 
actor to do all the work since spawning more actors causes 
unnecassary overhead of passing and waiting for messages. The 
master Actor divides the workload into 8 consequetive parts 
and distributes it to the 8 child. The 8 child Actors compute
the work given to them and print the starting number of the 
sequence. In case the range is not perfectly divisible by 8, 
then the remaining numbers, which will be less than 8, will be 
handled by one more child actor. Initially we started with 1 master 
and 4 child actors and went on to 6. On checking the performance,
we found significant difference on increasing the actors, so we
increased the number of child actors to 8. However on doubling 
it to 16, we found almost no change in performance and hence 
settled for 8 child actors and 1 master actor monitoring and
delegating work to the child actors.

On running scala project1 1000000 4, we get no result, indicating
there exists no series of 4 consequetive numbers till 1 million 
the square root of the squared sum of whose is an integer. 

Result for:
scala project1 1000000 4 
No result

6.64u 0.244s 0:04.05 169.975%    0+0k 0+64io 0pf+0w
Cores utilized=1.69 on a dual core machine but we get 3.4 on a Quad core machine. 

Largest problem solved:
scala project1 10000000 2400
