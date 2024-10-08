from de32_3rd_team5.utils.geoutil import loc_trans


def test_loc_trans():
    location = "36.57442742, 128.1593441"
    address = loc_trans(location)
    print(address)
    assert isinstance(address, str)
