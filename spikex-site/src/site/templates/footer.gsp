        <div id="footerwrap">
            <div class="container">
                <div class="row centered">
                    <div class="col-lg-4">
                        <p></p>
                    </div>

                    <div class="col-lg-4">
                        <p></p>
                    </div>
                    <div class="col-lg-4">
                        <p></p>
                    </div>
                </div>
            </div>
        </div>

        <div id="footer">
            <div class="container">
                <p class="muted credit"><small>&copy; 2016 NG Modular Oy
                    | Mixed with <a href="http://getbootstrap.com/">Bootstrap v3.2.0</a> 
                    | Baked with <a href="http://jbake.org">JBake ${version}</a>
                    </small>
                </p>
            </div>
        </div>

        <!-- Placed at the end of the document so the pages load faster -->
        <script src="<%if (content.rootpath) {%>${content.rootpath}<% } else { %><% }%>js/jquery-1.11.1.min.js"></script>
        <script src="<%if (content.rootpath) {%>${content.rootpath}<% } else { %><% }%>js/bootstrap.min.js"></script>
        <script src="<%if (content.rootpath) {%>${content.rootpath}<% } else { %><% }%>js/prettify.js"></script>

    </body>
</html>