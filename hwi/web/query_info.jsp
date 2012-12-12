<%--
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
--%>
<%@page import="org.apache.hadoop.hive.hwi.*" %>
<%@page import="org.apache.hadoop.hive.hwi.model.MQuery"%>
<%@page import="org.apache.hadoop.hive.conf.HiveConf"%>
<%@page import="org.apache.hadoop.hive.ql.session.SessionState"%>
<%@page errorPage="error_page.jsp" %>
<%
    String idStr = request.getParameter("id");
    Integer id = null;
    try {
        id = Integer.parseInt(idStr);
    } catch (Exception e) {
        
    }

	HiveConf hiveConf = new HiveConf(SessionState.class);
	QueryStore qs = new QueryStore(hiveConf);
	MQuery mquery = qs.getById(id);
	
	String[] jobIds = null;
	if(mquery.getJobId() != null){
		jobIds = mquery.getJobId().split(";");
	}
	
    String message = null;
    
%>
<!DOCTYPE html>
<html>
<head>
<title><%=mquery.getName()%></title>
<link href="css/bootstrap.min.css" rel="stylesheet">
</head>
<body style="padding-top: 60px;">
    <jsp:include page="/navbar.jsp"></jsp:include>
	<div class="container">
		<div class="row">
			<div class="span2">
				<jsp:include page="/left_navigation.jsp" />
			</div><!-- span2 -->
			<div class="span10">
				<h4>Query <%=mquery.getName()%></h4>

				<% if (message != null) { %>
				<div class="alert alert-info"><%= message %></div>
				<% } %>

				<%-- 
          	View JobTracker: <a href="<%= sess.getJobTrackerURI() %>">View Job</a><br>
          	Kill Command: <%= sess.getKillCommand() %>
          	 Session Kill: <a href="/hwi/session_kill.jsp?sessionName=<%=sessionName%>"><%=sessionName%></a><br>
          	--%>

			
			<dl class="dl-horizontal">
				<dt>Status</dt>
				<dd><%= mquery.getStatus() %></dd>
				
				<dt>Query</dt>
				<dd>
				<pre><%= mquery.getQuery() %></pre>
				</dd>
				
				<dt>Callback</dt>
				<dd><%= mquery.getCallback() %></dd>

				<dt>JobId</dt>
				<dd>
				<% if(mquery.getJobId() != null){ %>
				
				<% for(String jobId : jobIds){  %>
					<% if(!jobId.equals("")){ %>
					<a href="<%= HWIUtil.getJobTrackerURL(hiveConf, jobId) %>" target="_blank"><%= jobId %></a>
					<% } %>
				<% } %>
				
				<% }else{ %>
					null
				<% } %>
				</dd>
				
				<dt>Result location</dt>
				<dd>
                <%= mquery.getResultLocation() %>
                <% if (mquery.getStatus().equals(MQuery.Status.FINISHED)) { %>
                <a class="btn btn-small" href="query_result.jsp?id=<%= mquery.getId() %>" >View result</a>
                <% } %>
				</dd>
			
				<dt>Error message</dt>
				<dd><%= mquery.getErrorMsg() %></dd>
				
				<dt>Error code</dt>
                <dd><%= mquery.getErrorCode() %></dd>
			</dl>
				
			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>
