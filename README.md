# Simple CSV parser and query tool

## Running

The program loads the `data.csv` in the current working directory and
provides a tiny and simple REPL-like interface to perform queries.

The query language is dead simple and always requires two things:
`PROJECT`and `FILTER` to be specified within the query.

`PROJECT` lists the column names to output, and the `FILTER` lists the filter conditions for the data.

Running:

```sh
cargo run
```

Querying:

```sh
Welcome to the CSV data query tool!

 (CTRL-C for exit) REPL > PROJECT col1 FILTER col2="bar"

 {"col1": Integer(IntegerColumnType(2))}

 (CTRL-C for exit) REPL >
```

## Questions

### What were some of the tradeoffs you made when building this and why were these acceptable tradeoffs?

I simplified the way I work with the table and I don't require any order.
I also only work with `i64` integers and strings.
The filter iterator returns a hashmap, but there are other options, I
did not evaluate each one of them.

### Given more time, what improvements or optimizations would you want to add?

I'd first look at the filter iterator performance.

### When would you add them?

Didn't understand the question.

### What changes are needed to accommodate changes to support other data types, multiple filters, or ordering of results?

It depends. There is not just one way to do all of this, and never is
one just always better. We could store the index to the data entries and
the data itself separately instead of a hashmap. We could use vectors,
for example, this would be much more efficient for the CPU and better
with regards to the memory accesses, less latency and jumps.

### What changes are needed to process extremely large datasets?

There are millions of ways to achieve that, depending on the desired
outcome and the actual problem at that time.

Perhaps, not even loading the data set into RAM in the first place, but
rather locking it on the filesystem (if it is a file), or locking the
parts of it, if possible, and going through it in the filter iterator.

Then, avoiding copies, and using some compression goes there.

There are many books on how to write a database out there, I am too lazy
to repeat everything written there and all the old and new, bad and good
practices to do the things. Only reinvent the wheel if you know all the
other wheels.

### What do you still need to do to make this code production ready?

I doubt there are just CSV parsers like that :-D This question is too
broad and there are millions of choices to make depending on the exact
"production" requirements. It is impossible to answer unless we discuss
the exact requirements first. There can't be one single solution to fit
all, or it would take unreasonably long time to implement while the
business actually doesn't need it.
