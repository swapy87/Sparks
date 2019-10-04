package test

object FunctionExample {
  
  def  fun() 
  {
    println("Bello from fun")
  }
  def sum(a:Int,b:Int) : Int=
  {
    //println("Sum "+(a+b));
   val sum= a+b
   println("sum------->"+sum)
   a+b
  }
  
  
  def main(args: Array[String])
  {
    fun()
    println(sum(args(0).toInt,args(1).toInt))
    println("Main")
  }
  
  
}