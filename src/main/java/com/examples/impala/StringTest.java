package com.examples.impala;


import java.util.regex.Pattern;

public class StringTest {

	public static void main(String[] args) {
		String s = "&lt;p&gt;According to Mark Russo (&lt;a href=&quot;http://introducinglinq.com/blogs/marcorusso/archive/2008/07/20/use-of-distinct-and-orderby-in-linq.aspx&quot;&gt;Introduction To Linq&lt;/a&gt;) &lt;/p&gt;&#xA;&#xA;&lt;p&gt;&lt;strong&gt;The problem is that the Distinct&#xA; operator does not grant that it will&#xA; maintain the original order of&#xA; values.&lt;/strong&gt;&lt;/p&gt;&#xA;&#xA;&lt;p&gt;So your query will need to work like this&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;var names = (from DataRow dr in dataTable.Rows&#xA;             select (string)dr[&quot;Name&quot;]).Distinct().OrderBy( name =&amp;gt; name );&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;";
		String regex = "[&lt;]*[&gt;]*";
		
		System.out.println(Pattern.compile(regex).matcher(s).replaceAll(""));

	}

}
