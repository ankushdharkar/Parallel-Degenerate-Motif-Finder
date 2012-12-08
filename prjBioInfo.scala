import scala.actors.remote.RemoteActor
import scala.actors.remote.RemoteActor._
import scala.actors.remote.Node
import scala.actors.Actor._
import scala.actors._
//import scala.math._
import scala.util.Random
import scala.io.Source._
import scala.collection.mutable._
import java.io._

case class msgStart(fname:String)
case class msgInitializeAmino(str:String,lvl:Int)
case class msgAddAddr(newip: String)
case class msgDie()
case class msgOver()
case class msgSetParams(id:Int,nw : Int)
case class msgAskCall()
case class msgTell()
case class msgIamDone()
case class msgMotif(m:String,n:Int)
case class msgFindMotifInString(mtf: String,fname:String)
case class msgReportIndex(lno: Int,start: Int, end: Int)


class InfoAnk(l:Int,s:Int,e:Int) {
  var lineno=l
  var start=s
  var end=e
}


class NodeOfTree {
	var data = -1
			var linkList = new scala.collection.mutable.ListBuffer[EdgeOfTree]()
}

class EdgeOfTree {
	var str: String = ""
			var node: NodeOfTree = null
}

class newTree {

}

class Daemon(myIp : String) extends Actor {

			var numDaemons = 0 
			var myId = 0
		
			var lMap : scala.collection.mutable.HashMap[String, Int] = null
			var tempMap : scala.collection.mutable.HashMap[String, Int] = null
			
			var addrList = new scala.collection.mutable.ListBuffer[String]()
			var strmap = new scala.collection.mutable.HashMap[String, Int]()
			var strcommonmap = new scala.collection.mutable.HashMap[String, Int]()

			var nomsgs = 0
			var seqNo = 0
			var currPhase = 0
			var obj : scala.actors.OutputChannel[Any] = null
			
			var busy:Boolean = false
			
			var degenMap: scala.collection.mutable.HashMap[String, scala.collection.mutable.ListBuffer[String]] = null
			var degenMatrix: Array[Array[Int]] = null
			var characterToIndexMap: scala.collection.mutable.HashMap[String, Int] = null
			var characterToIndexReverseMap: scala.collection.mutable.HashMap[Int, String] = null

			def formCharacterToIndexMap(typ: String) = {

		characterToIndexMap = scala.collection.mutable.HashMap[String, Int]()
				characterToIndexReverseMap = scala.collection.mutable.HashMap[Int, String]()


				if (typ.equals("A")) {

					characterToIndexMap += ("C" -> 0)
							characterToIndexMap += ("S" -> 1)
							characterToIndexMap += ("T" -> 2)
							characterToIndexMap += ("P" -> 3)
							characterToIndexMap += ("A" -> 4)
							characterToIndexMap += ("G" -> 5)
							characterToIndexMap += ("N" -> 6)
							characterToIndexMap += ("D" -> 7)
							characterToIndexMap += ("E" -> 8)
							characterToIndexMap += ("Q" -> 9)
							characterToIndexMap += ("H" -> 10)
							characterToIndexMap += ("R" -> 11)
							characterToIndexMap += ("K" -> 12)
							characterToIndexMap += ("M" -> 13)
							characterToIndexMap += ("I" -> 14)
							characterToIndexMap += ("L" -> 15)
							characterToIndexMap += ("V" -> 16)
							characterToIndexMap += ("F" -> 17)
							characterToIndexMap += ("Y" -> 18)
							characterToIndexMap += ("W" -> 19)

							characterToIndexReverseMap += (0 -> "C")
							characterToIndexReverseMap += (1 -> "S")
							characterToIndexReverseMap += (2 -> "T")
							characterToIndexReverseMap += (3 -> "P")
							characterToIndexReverseMap += (4 -> "A")
							characterToIndexReverseMap += (5 -> "G")
							characterToIndexReverseMap += (6 -> "N")
							characterToIndexReverseMap += (7 -> "D")
							characterToIndexReverseMap += (8 -> "E")
							characterToIndexReverseMap += (9 -> "Q")
							characterToIndexReverseMap += (10 -> "H")
							characterToIndexReverseMap += (11 -> "R")
							characterToIndexReverseMap += (12 -> "K")
							characterToIndexReverseMap += (13 -> "M")
							characterToIndexReverseMap += (14 -> "I")
							characterToIndexReverseMap += (15 -> "L")
							characterToIndexReverseMap += (16 -> "V")
							characterToIndexReverseMap += (17 -> "F")
							characterToIndexReverseMap += (18 -> "Y")
							characterToIndexReverseMap += (19 -> "W")
				}


		if (typ.equals("N")) {
			characterToIndexMap += ("A" -> 0)
					characterToIndexMap += ("C" -> 1)
					characterToIndexMap += ("G" -> 2)
					characterToIndexMap += ("T" -> 3)

					characterToIndexReverseMap += (0 -> "A")
					characterToIndexReverseMap += (1 -> "C")
					characterToIndexReverseMap += (2 -> "G")
					characterToIndexReverseMap += (3 -> "T")
		}





	}

	def formMatrix(fileName: String) = {

		degenMatrix = new Array[Array[Int]](20)

				for (i <- 0 until degenMatrix.size) {
					degenMatrix(i) = new Array[Int](20)
				}

		val brLines = new BufferedReader(new FileReader(fileName))
		var lstr = brLines.readLine()
		var numLines = 0
		var row = 0
		var col = 0

		while (lstr != null) {

			degenMatrix(row)(col) = lstr.toInt
					lstr = brLines.readLine()
					col = col + 1

					if (col == 20) {
						col = 0
								row = row + 1
					}
		}

		for(i<- 0 until degenMatrix.size ){
     		for(j<- 0 until degenMatrix(i).size ){
     			print(degenMatrix(i)(j) + " ") 
     		}
     		println()
     }
	}

	def formDengList(typ:String,minLimit: Int) = {
		degenMap = new scala.collection.mutable.HashMap[String, scala.collection.mutable.ListBuffer[String]]()


				if (typ.equals("A")) {
					for (everyAmino <- characterToIndexMap.keySet) {
						val aminIndx = characterToIndexMap(everyAmino)

								var tempList = new scala.collection.mutable.ListBuffer[String]()

								for (i <- 0 until degenMatrix(aminIndx).size) {
									if (degenMatrix(aminIndx)(i) >= minLimit) {
										tempList += characterToIndexReverseMap(i)
									}
								}

						degenMap += (everyAmino -> tempList)

					}
				}


		if (typ.equals("N")) {
			for (elem <- characterToIndexMap.keySet) {
				val templist = new scala.collection.mutable.ListBuffer[String]()
						templist += elem
						degenMap += (elem -> templist)
			}
		}


	}


	def formDegenSet(str:String): scala.collection.mutable.ListBuffer[String] = {

		var sset = scala.collection.mutable.ListBuffer[String]()
				sset += ""

				for (i <- 0 until str.length()) {
					var tempset = scala.collection.mutable.ListBuffer[String]()

							val thelist = degenMap("" + str.charAt(i))

							for (evryitm <- sset) {
								// println("Temp item : "+evryitm)
								for (evelem <- thelist) {
									tempset += (evryitm + evelem)
											// println("Adding \t: "+evryitm+evelem)
								}

								// println("removing : "+evryitm)  
								sset -= evryitm
							}

					// println("Add ALL \t : "+tempset)

					for (itm <- tempset) {
						sset += itm
					}

				}

		return sset

	}


	def degenEquivalent_old(str1:String, str2: String): Boolean = {

		var sset = formDegenSet(str1)

				if(sset.contains(str2)){
					return true
				}

		return false
	}

	def searchDegenMap_old(map1: scala.collection.mutable.HashMap[String, Int], str: String): String = {

		var sset = formDegenSet(str)

				for (evitm <- sset) {
					if (map1.keySet.contains(evitm)) {
						return evitm
					}
				}

		return null
	}


	
	//Faster
	def degenEquivalent(str1:String, str2: String): Boolean = {
				//str1 will be checked with str2
	  
	  var cnt =0
	  var matchlen = 0
	  
	  while(cnt < str1.length() && cnt < str2.length() ){
	    if(  degenMap(""+str2.charAt(cnt)).contains(""+str1.charAt(cnt)) ){
	   	matchlen = matchlen + 1 
	    }
	     cnt = cnt+1
	  }	  

		if(matchlen == str1.length() && matchlen == str2.length()){
			return true
		}

		return false
	}

	
	def searchDegenMap(map1: scala.collection.mutable.HashMap[String, Int], str: String): String = {

	  for (evitm <- map1.keySet) {
					if (degenEquivalent(str,evitm)) {
						return evitm
					}
				}

		return null
	}

	
	

	def localMotifs(srchFile:String): scala.collection.mutable.HashMap[String, Int] = {
			
			val brLines = new BufferedReader(new FileReader(srchFile))
			var lstr = brLines.readLine()
			var numLines = 0

			while (lstr != null) {
				numLines = numLines + 1
						lstr = brLines.readLine()
			}
			brLines.close()
			//println("(" + myId + ")"+numLines)

					var s = 0
					var e = 0

					if (myId < (numLines % numDaemons)) {
						s = myId * ((numLines / numDaemons) + 1) + 1
								e = s + ((numLines / numDaemons) + 1 - 1)
					} else {
						s = (myId * (numLines / numDaemons)) + (numLines % numDaemons) + 1
								e = s + ((numLines / numDaemons) - 1)
					}

			//println("(" + myId + ")"+" S = " +s)    
			//println("(" + myId + ")"+" E = " +e+"\n")    

			val brSrch = new BufferedReader(new FileReader(srchFile))
			var srchstr = brSrch.readLine()
			var count = 1

			//To reach till start point  
			while (srchstr != null && count < s) {
					srchstr = brSrch.readLine()
						count = count + 1
			}

			//println("(" + myId + ") : "+" First Line = "+srchstr)
			
			count = 0

					if (e - s >= 0) {
						
								//println(srchstr)

								for (k <- 0 until srchstr.length()) {
									for (l <- k until srchstr.length()) {
										var substr = srchstr.substring(k, l + 1)
												//println(substr)
												var num: Int = 1

												var substr_in_map = searchDegenMap(strmap, substr)

												if (substr_in_map != null) {
													num = num + strmap(substr_in_map)
															substr = substr_in_map
												}

										strmap += (substr -> num)
									}
								}

						count = count + 1
						srchstr = brSrch.readLine()
						
					}

			
      	println("\n(" + myId + ")"+" Basic compare set created, discarding non-motives ... ")
      

			if (e - s >= 1) {
				while (srchstr != null && count <= (e - s)) {
					count = count + 1
						

							for (k <- 0 until srchstr.length()) {
								for (l <- k until srchstr.length()) {
									var substr = srchstr.substring(k, l + 1)

											var substr_in_map = searchDegenMap(strmap, substr)
											var substr_in_c_map = searchDegenMap(strcommonmap, substr) //just to find a matching number

											//println(substr_in_map+ " : "+ substr_in_c_map)

											if (substr_in_map != null) { //strmap.keySet.contains(substr)    ) { 

												var num = 1
														if (substr_in_c_map != null) { //strcommonmap.contains(substr)     ) { 
															num = num + strcommonmap(substr_in_c_map)
														}

												strcommonmap += (substr_in_map -> num) //Careful here

														var numo = strmap(substr_in_map) - 1

														if (numo == 0) {
															strmap -= substr_in_map
														} else {
															strmap += (substr_in_map -> numo)
														}
											}

								}
							}

					
				   srchstr = brSrch.readLine()
					
				   strmap = strcommonmap
					strcommonmap = new scala.collection.mutable.HashMap[String, Int]()
				}
			}

			for (elem <- strmap.keySet) {
				println("(" + myId + ")\t" + elem + "\t" + strmap(elem))
			}

			return strmap


	}


	
	def findMotifsInString(mtf:String,srchFile:String): Array[Int] = {
			
			val brLines = new BufferedReader(new FileReader(srchFile))
			var lstr = brLines.readLine()
			var numLines = 0

			while (lstr != null) {
				numLines = numLines + 1
						lstr = brLines.readLine()
			}
			brLines.close()
	  
	  
			var s = 0
			var e = 0

					if (myId < (numLines % numDaemons)) {
						s = myId * ((numLines / numDaemons) + 1) + 1
								e = s + ((numLines / numDaemons) + 1 - 1)
					} else {
						s = (myId * (numLines / numDaemons)) + (numLines % numDaemons) + 1
								e = s + ((numLines / numDaemons) - 1)
					}

			//println(" S = " +s)    
			//println(" E = " +e+"\n")    

			val brSrch = new BufferedReader(new FileReader(srchFile))
			var srchstr = brSrch.readLine()
			var count = 1

			//To reach till start point  
			while (srchstr != null && count < s) {
				srchstr = brSrch.readLine()
				count = count + 1
			}
	
		
			while (srchstr != null && count <= e) {
				
			  	//println(srchstr)
			  for (k <- 0 until srchstr.length()) {
					for (l <- k until srchstr.length()) {
						var substr = srchstr.substring(k, l + 1)
						
						if(degenEquivalent(substr,mtf)==true){
						   //println(count +"\t"+substr+"\t\t("+k+","+l+")")
						   obj ! msgReportIndex(count,k,l)
						}
					}
				}
			  	
			  srchstr = brSrch.readLine()
			  count = count + 1
			  
			}
			brSrch.close()
			
			return null
			
	}
	
	
	
	



	def act() {
		  println("Actor at IP   " + myIp + "   started succesfully")

		// val bw1 = new BufferedWriter(new FileWriter(myIp + ".sclog", true))
		// bw1.write(myIp + " Started\n");
		// bw1.close()

		RemoteActor.classLoader = getClass().getClassLoader()

				val spString = myIp.split(":")
				alive(spString(1).toInt)
				register('daemon, self)


		loop {

			react {
			  
			  case msgSetParams(id,nw)=>{
			    myId = id
			    numDaemons = nw
			    obj = sender
			    currPhase = 0
			  }  
			  
			  
			case msgInitializeAmino(fname,lvl) => {
				formMatrix(fname)
				formCharacterToIndexMap("A")
				formDengList("A",lvl)   //level does not matter with nucleotide config

					for(elem <- degenMap.keySet){
						print( elem + " := ")

						for(i <- 0 until degenMap(elem).length){
							print(degenMap(elem)(i)+"  ")
						}

						print("\n") 
					}
				

			println( "("+myId+") Ready")
				
			}

			case msgStart(fname) => {
			  println("("+myId+") : "+"Creating the Local Motif set")
				lMap = localMotifs(fname)
				
			
				if(lMap.size > 0){
					println("("+myId+") : "+"Formed local motif set, Initiating Transfer...")
					self ! msgAskCall()
				}
				else{
				  println("("+myId+") : "+"Idle : No Elements Formed")
				}
			
			}

			
			case msgAskCall() =>{
			  
			  //println("("+myId+") : "+" AskCall()  Phase = "+ currPhase+"/"+( scala.math.log(numDaemons) / scala.math.log(2) ).toInt)
			  
				if (currPhase <  ( scala.math.log(numDaemons) / scala.math.log(2) ) ){
				  
					val targetId = (myId+scala.math.pow(2,currPhase)).toInt
				  
				  if( (myId % scala.math.pow(2,(currPhase+1) )) == 0  && targetId < numDaemons){
				    
				    val brSrch = new BufferedReader(new FileReader("ip.dat"))
					 var srchstr = brSrch.readLine() //Ignore line 1
					 var cnt = 0
					 
					  while (srchstr != null && cnt < targetId){
						  srchstr = brSrch.readLine()
						  cnt=cnt+1
					  } 
					 
				    val destnIp = srchstr.split(":")
					 val nd = Node(destnIp(0),destnIp(1).toInt)
					 val tempdaemonactor = select(nd, 'daemon)
					 
					// println("("+myId+") : "+"Requesting from "+targetId)
					 tempMap = new scala.collection.mutable.HashMap[String, Int]() //form a new empty receiving map
					 //Thread.sleep(2000) // so that the other guy at least starts 
					 tempdaemonactor ! msgTell()
					 
				  }
				  else{
			     }
			 	}
				else{
				  //No More asking !
				  
				  //println("("+myId+") : "+"DONE!")
				  
				  if(myId == 0){
					 // println("Phase No. : "+currPhase)
					 // println("MOTIFS : ")
				  
					  for(mtf <- lMap.keySet){
						  //println("("+myId+") : -->"+"\t"+itm+"\t"+lMap(itm))
						  	obj  ! msgMotif(mtf,lMap(mtf))
					  }
					  
					  obj ! msgIamDone() 
					  
				  }
				}
				
			}
				
			case msgTell() =>{
			  
			  //no. of times not considered
				//println("("+myId+") : "+"Request Received")
			  for( mtf <- lMap.keySet){
				  sender  ! msgMotif(mtf,lMap(mtf))
			  }
				sender ! msgIamDone()
				println("("+myId+") : "+"Transfer Completed ")
			  	 
			} 
			
			
			case msgMotif(str,i) =>{
			 // println("("+myId+") : "+"Received : " + str + "  " + i )
			  tempMap += ( str -> i )
			}
			
			case msgIamDone() =>{
			
			  println("("+myId+") : "+"n(lMap) = "+lMap.keySet.size+"\tn(tempMap) = "+tempMap.keySet.size) 
			
			  
			  var newMap = new scala.collection.mutable.HashMap[String, Int]() //form a new empty receiving map
			  
				
				println("("+myId+") : "+"Intersection  "+(myId+scala.math.pow(2,currPhase)).toInt) 
				
				
			  for(itm <- tempMap.keySet){
			    
			     var lstr = searchDegenMap(lMap,itm)
			     
			     if(lstr != null){
			   	  val incnum = tempMap(itm)
			   	  val lnum = lMap(lstr)
			   	  var minnum = 0
			   	  
			   	  
			   	  if(incnum < lnum){
			   	    minnum = incnum
			   	  }
			   	  else{
			   	    minnum = lnum
			   	  }
			   	  
			   	  newMap += (lstr -> minnum)
			   	  
			   	  if(lnum-minnum > 0){
			   	    lMap += (lstr-> (lnum-minnum))
			   	  }
			   	  else{
			   	    lMap -= lstr
			   	  }
			     }
			  }
			  
			  
				lMap = newMap
				println("("+myId+") : "+"n(new lMap) = "+lMap.keySet.size)
				
				/*for(itm <- lMap.keySet){
				   println("("+myId+") : "+"\t"+itm+"\t"+lMap(itm))
				 }*/
				
			  //common of lmap and tempmap
			  //make it lmap
				
			  currPhase = currPhase + 1 
				self ! msgAskCall()
			}
			
			
			case msgFindMotifInString(mtf,fname) =>{
				findMotifsInString(mtf,fname)
				obj ! msgReportIndex(0,0,0)
			}
			
			case msgAddAddr(newip) => {
				addrList += newip
			}

			case msgDie() => {
				println(myIp + " killed prematurely")
				exit()
			}

			case msg => {
				println("("+myId+") : "+"Received Default msg")
			}

			}

		} //loop  
	} //act
} //class


object startdaemon {
	def main(args: Array[String]) {
		if (args.length != 1) { println("Argument missing : [IP:PORT_no]") }
		else if (args.length == 1) {
		  val d = new Daemon(args(0))
		  d.start
		}
	}
}




object startobj {
	def main(args: Array[String]) {

	  var time1:Long =0
	  var time2:Long =0
	  
	  
	  
		if (args.length != 1) { println("Argument missing : [number of actors]") }
		else if (args.length == 1) {
		  
		  RemoteActor.classLoader = getClass().getClassLoader()
		  
		 time1 = System.currentTimeMillis()
		  
		  
		  
			val actSize = args(0).toInt;
			var d = new Array[String](actSize)

					val brip = new BufferedReader(new FileReader("ip.dat"))
					
					for (i <- 0 until actSize) {
						d(i) = brip.readLine()
					}
		
			
					brip.close()

			for (i <- 0 until actSize) {
				val spString = d(i).split(":")
						val nd = Node(spString(0), spString(1).toInt)
						val tempdaemonactor = select(nd, 'daemon)
						tempdaemonactor ! msgSetParams(i,actSize)
						tempdaemonactor ! msgInitializeAmino("BLOSUM62.dat",2)
						//println("Starting ...")
						tempdaemonactor ! msgStart("Proteins.txt")
			}

			/*receiveWithin(300) {
			case TIMEOUT => {
				println("   WATCHOUT !  ")
				Thread.sleep(100)
			}
			}*/
			
			var flag=0
			var totcount =0 
			
			
			while(flag==0){
				receive{
				  case msgMotif(s,n) =>{
				    
				    var spc=""
				    for(i<-0 until (20-s.length())){
				      spc = spc + " "
				    }
				   
				    
				    
				    for (i <- 0 until actSize) {
				   	 val spString = d(i).split(":")
				   	 val nd = Node(spString(0), spString(1).toInt)
				   	 val tempdaemonactor = select(nd, 'daemon)
				   	
				   	 tempdaemonactor ! msgFindMotifInString(s,"Proteins.txt")
				   	 totcount = totcount + 1
				    }
			
				    println("\t    "+s+spc+"\t"+n)
				  }	
				 
				  case msgIamDone() =>{
				    flag=1
				  }
				  
				}			  
			}
			
			println("Motifs Formed : Done !")
			
			println("\nTotal Count = "+totcount)
			//Thread.sleep(3000)
			
			var infoarr = new scala.collection.mutable.ListBuffer[InfoAnk]()
			
			
			flag=0
			
			 while(flag < totcount) {
				 receive{
				   case msgReportIndex(lin,strt,ed) =>{
						
						 if(lin == 0 && strt ==0 && ed == 0){
							 	flag= flag+1
							 	//println("============>"+flag)
						 }
						 else{
						    //println(lin +"\t\t("+strt+","+ed+")")
						 
						    var newAnk = new InfoAnk(lin,strt,ed)
						    
						    var bflg=0
						   
						    for(ex <- infoarr){
						   	if(ex.lineno == lin){
						   			if(strt >= ex.start && ed <=ex.end){
						   			  //println("Exists as : (" +ex.start+","+ex.end+")")
						   			  bflg =1
						   			}
						   			
						   		  if(strt <= ex.start && ed >=ex.end){
						   			  //println("Replacing  : (" +ex.start+","+ex.end+")")
						   			  infoarr -= ex
						   			}
						   	} 
						   	
						    }
						    
						    if(bflg == 0 ){
						      infoarr += newAnk
						    }
						    else{
						     //println("Rejected !")
						    }
						    
						 }
					 }
		
				 }
			 }
			
			println("The End of Info")
			
			for(itm <- infoarr){
			  println(itm.lineno+"\t("+itm.start+","+itm.end+")")
			}
			
			
			
			val bw = new BufferedWriter(new FileWriter("output.dat"))
			val br = new BufferedReader(new FileReader("Proteins.txt"))

			 var str = br.readLine()
			 var lnx = 1
			 
			 while( str != null){
				 
			   var strb = new StringBuilder()
			   
			   for(idx <- 0 until str.length()){
			   	
			    
			     var fg = 0
			     			     
			     for(itm <- infoarr){
			   		if(itm.lineno == lnx){
			   	
			   		  if(itm.start==idx){
			   		    strb.append("(")
			   		    strb.append(str.charAt(idx))
			   		    fg=1
			   		  }
			   		  	
			   		  

			   		  if(itm.end==idx){
			   		    
			   		    if(fg==0){
			   		   	 strb.append(str.charAt(idx))
			   		    }
			   		    strb.append(")")
			   		    fg=1
			   		  }

			   		  

			   		}
			   	}
			   
			     if(fg==0){
			   	  strb.append(str.charAt(idx))
			     }
			     
			   //print(str)
			   }
			   
			   bw.write(strb.toString()+"\n")
			   
			   str = br.readLine()
			   lnx=lnx+1
			 }
			
			bw.close()
			br.close()
			
			
			time2 = System.currentTimeMillis()
			
			println("\nTime taken = " + ((time2-time1)/1000) + " s")
			
			println("Output Done !  check output.dat ")
			
		}//if_correct_arg
	}
}