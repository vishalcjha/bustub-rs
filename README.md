### bustub-rs

It is rust clone of [cmu bustub db](https://github.com/cmu-db/bustub/tree/master).
If you want to help or contribute please reach out.

## how will this progress
My plan is to follow CMU db course on You-Tube. There are project in course page, which will work as milestone  for this repo. I will try to write similar test case.

## Current status
* Project 1 
  * Lruk Replacer - Done
  * Disk Scheduler - TODO 
  * Buffer pool manager - TODO

## Some rust specific learning on the way
<p>Sync- Sync is easy to reason. Can we have two threads have copy of same object and operate on them. & is sync as it can not modify state of object while &mut is not as it has ability to write. 
<p>Send - this is bit tedious but it reasons about ownership. If there is no shared reference in a object, it is Send. But if there is shared ownership it is not that easy.
<p>For both Sync and Send, compiler decides what a given type is, unless overwritten by dev. Following table summarizes it.

|Type      |Sync                      |Send                        |
|----------|--------------------------|----------------------------|
|RC        |No                        | No                         |
|ARC       |If T is sync and send     | If T is sync and send      |
|Mutex     |If T is send              | If T is send               |
|Cell      |No                        | If T is send               |
|UnsafeCell|No                        | If T is send               |
|RawPtr    |No                        | No                         |

RawPtr need not be Send but it is more a cautionary one. If you have a type which RawPtr one should claim it Sync and Send status without rust compiler figuring it out.
Mutex is Send but Mutex guard is not. The reason being implementation by OS. If lock is grabbed by one thread, same thread should be responsible for releasing it.

<p>
std::mem::transmute hack. 
Looks in page_guard.rs how it allows to hold LockGuard without lifetime type annotation. Just a hack for fun.

<p>

`matches!() `  - returns if given expression matches provided pattern. Check its use in `data_type` create.

<p>