package com.tfitter.tokenizer

sealed abstract class TwitToken
case class TwitTokenUser(s: String) extends TwitToken
case class TwitTokenTag(s: String)  extends TwitToken
case class TwitTokenUrl(s: String)  extends TwitToken
case class TwitTokenWord(s: String) extends TwitToken
case class TwitTokenNone() extends TwitToken

//object TwitTokens {
	// val urlChars = Set(":/.~".toArray:_*)
	// "/:.~" contains s(i) 
	// TODO class TwitTokens cannot see this from companion object?
//	val httpSb = new StringBuilder("http")
//}

class TwitTokens(val s: String) extends Iterator[TwitToken] {
	val length = s.length
	var i = 0
	var isUrl = false
	val sb = new StringBuilder // default size 16
	
	def hasNext: Boolean = i < length
	
	def reachToken: Unit = {
		while (i < length && !s(i).isLetter && "@#".indexOf(s(i)) < 0)
			i += 1
	}
	
	def fillToken: Unit = {
		sb.delete(0,sb.length)
		isUrl = false

		while (i < length && (s(i).isLetter || 
			(sb.length == 0 && "@#".indexOf(s(i)) >= 0) ||
			(isUrl  && "/:.~".indexOf(s(i)) >= 0))) { 
				sb.append(s(i))
				i += 1
				if (sb.length == 4 && sb.toString == "http") isUrl = true
		}
		
		reachToken // so hasNext will be correct right away
	}
	
	def next: TwitToken = {
		if (!hasNext) throw new java.util.NoSuchElementException
		// TODO raise exception on empty?
		fillToken
		if (sb.length < 2) TwitTokenNone() // without () a problem!
		else if (sb(0) == '@') TwitTokenUser(sb.toString)
		else if (sb(0) == '#') TwitTokenTag(sb.toString)
		else if (isUrl) TwitTokenUrl(sb.toString)
		else TwitTokenWord(sb.toString)
	}
	
	// body -- get to the first token
	reachToken
}
