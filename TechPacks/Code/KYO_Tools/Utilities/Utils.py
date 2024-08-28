def fileToXMLObject(xmlfile):
    '''Convert File to XML Object'''
    # from xml.etree.ElementTree import ElementTree
    # return ElementTree(file=xmlfile)

    import io
    from xml.etree import ElementTree as ET

    with io.open(xmlfile, 'r') as f:
        contents = f.read()
        return ET.fromstring(contents)


def fileToXMLEvents(xmlfile):
    from xml.etree.ElementTree import iterparse
    context = iterparse(xmlfile, events=("start", "end"))
    return context

def removesSpecialCharacters(text):
    '''return text that is safe to use in an XML string'''
    html_escape_table = {
        "\x96": " ",
        "\xa0": " ",
        "›": "",
        "\x85": "...",
    }
    return "".join(html_escape_table.get(c, c) for c in str(text))

def escape(text):
    '''return text that is safe to use in an XML string'''
    try:
        text = str.encode(text, 'utf8').decode("utf-8")
    except:
        pass
    text = removesSpecialCharacters(text)
    html_escape_table = {
        "&": "&amp;",
        '"': "&quot;",
        ">": "&gt;",
        "<": "&lt;",
    }
    return "".join(html_escape_table.get(c, c) for c in str(text))


def unescape(s):
    '''change XML string to text'''
    if s is None:
        s = ''

    s = s.replace("&lt;", "<")
    s = s.replace("&gt;", ">")
    s = s.replace("&apos;", "'")
    s = s.replace("&quot;", '"')
    s = s.replace("&amp;", "&")
    return s