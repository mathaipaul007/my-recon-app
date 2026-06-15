from py_mini_racer import py_mini_racer

ctx = py_mini_racer.MiniRacer()

ctx.eval("""
function doProcess(...args){
    if(args.length > 0){
        // Example: could process args here
    }
    return args;
}
""")

values = ["Madhu", 0, "is", "a", "good", "boy"]
result = ctx.call("doProcess", *values)
print(result)
