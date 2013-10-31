import akka.actor._

object project1 extends App {
  var n: Long = args(0).toLong;
  var k: Int = args(1).toInt
  //println("Range = " + n)
  //println("objects in Sequence = " + k)
  val system = ActorSystem("FirstSystem");
  val Master = system.actorOf(Props(new Master(k, n)), name = "Master");
  Master ! "Start"
}

class Worker extends Actor {

  def receive = {
    case (start: Long, end: Long, k: Int) =>

      for (mstart <- start to end) {

        var sum: BigInt = 0

        for (i: Long <- mstart to (mstart + k - 1)) {
          sum += (i * i)
        }
        var sqrt: BigInt = ((Math.sqrt(sum.toDouble) + 0.5).asInstanceOf[Long])

        if (sqrt * sqrt == sum) {
          println(mstart);
        }

      }
      sender ! "Done";
  }
}

class Master(k: Int, n: Long) extends Actor {
  var count: Int = 0;
  def receive = {
    case "Start" =>
      val system = ActorSystem("SecondSystem");

      var remainder: Long = n % 8;
      var End: Long = n - remainder
      var Start: Long = 1;
      var zero: Long = 0;

      if (n > 6) {
        var slave1 = system.actorOf(Props[Worker], name = "slave1");
        var slave2 = system.actorOf(Props[Worker], name = "slave2");
        var slave3 = system.actorOf(Props[Worker], name = "slave3");
        var slave4 = system.actorOf(Props[Worker], name = "slave4");
        var slave5 = system.actorOf(Props[Worker], name = "slave5");
        var slave6 = system.actorOf(Props[Worker], name = "slave6");
        var slave7 = system.actorOf(Props[Worker], name = "slave7");
        var slave8 = system.actorOf(Props[Worker], name = "slave8");

        count += 8

        var part1: Long = n / 8;
        var part2: Long = n / 4;
        var part3: Long = 3 * (n / 8);
        var part4: Long = n / 2;
        var part5: Long = 5 * (n / 8);
        var part6: Long = 3 * (n / 4);
        var part7: Long = 7 * (n / 8);

        slave1 ! (Start, part1, k);
        slave2 ! (part1 + 1, part2, k);
        slave3 ! (part2 + 1, part3, k);
        slave4 ! (part3 + 1, part4, k);
        slave5 ! (part4 + 1, part5, k);
        slave6 ! (part5 + 1, part6, k);
        slave7 ! (part6 + 1, part7, k);
        slave8 ! (part7 + 1, End, k);
      }

      if (remainder > 0 || End == zero)
        count += 1;

      if (remainder != 0 || End == zero) {
        var slave9 = system.actorOf(Props(new Worker), name = "slave9");
        slave9 ! (End + 1, n, k);
      }

    case (done: String) =>
      count -= 1;
      if (count == 0) {
        exit()
        //context.system.shutdown()
      }

  }
}


