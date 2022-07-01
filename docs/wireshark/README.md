# Wireshark dissector

[`udx.lua`](udx.lua) is a dissector for [Wireshark](https://www.wireshark.org/) that will detect and parse UDX packets. To use it simply move the file into the [Wireshark plugin folder](https://www.wireshark.org/docs/wsug_html_chunked/ChPluginFolders.html). After restarting Wireshark or pressing `Ctrl-Shift-L` to reload the plugins UDX packets should be detected and parsed.
