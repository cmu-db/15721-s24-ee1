---
marp: true
theme: default
class: invert # Remove this line for light mode
paginate: true
---


# Execution Engine

<br>

Connor, Kyle, Sarvesh


---


# This is a Title!

This is not a title


---



# **This is a different color Title!**

_This is_ **just markdown**


---


# This is Ferris!

![bg right:50% 80%](./images/ferris_happy.svg)



---


# Example slides from Rust StuCo incoming:


---


# `if` Expressions

![bg right:25% 75%](./images/ferris_does_not_compile.svg)

`if` expressions must condition on a boolean expression.

```rust
fn main() {
    let number = 3;

    if number {
        println!("number was three");
    }
}
```

```
error[E0308]: mismatched types
 --> src/main.rs:4:8
  |
4 |     if number {
  |        ^^^^^^ expected `bool`, found integer
```


---


# Matches Are Exhaustive

![bg right:25% 75%](../images/ferris_does_not_compile.svg)

The `match` patterns must cover all possible values that the matched expression may take.

What happens when we miss a case?

```rust
let x: i8 = 5;
let y: Option<i8> = Some(5);

let sum = match y {
    Some(num) => x + num,
};
```


---


# Matches Are Exhaustive

```rust
let x: i8 = 5;
let y: Option<i8> = Some(5);

let sum = match y {
    Some(num) => x + num,
};
```

```
error[E0004]: non-exhaustive patterns: `None` not covered
   --> src/main.rs:6:21
    |
6   |     let sum = match y {
    |                     ^ pattern `None` not covered
```

* Forces us to explicitly handle the `None` case
* Protecting us from the billion-dollar mistake!


---





---





---





---